//! Durable, OS-authenticated local hub administration.
//!
//! Only SHA-256 digests reach SQLite. A newly generated credential is delivered
//! to a private, create-new file and synchronized before its transaction commits.
//! A process crash can leave an uncommitted output file; it cannot commit an
//! undelivered credential. Administration trusts the effective OS user.

use rand::{rngs::OsRng, RngCore};
use rusqlite::{params, Connection, OpenFlags, Transaction, TransactionBehavior};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Errors contain operational context, never credential material.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

const DATABASE_NAME: &str = "hub.sqlite3";
const MAX_REGISTRATIONS: usize = 10_000;
const SCHEMA: &str = "
CREATE TABLE registrations (
    id TEXT PRIMARY KEY NOT NULL CHECK(length(id) BETWEEN 1 AND 63),
    label TEXT NOT NULL CHECK(length(label) BETWEEN 1 AND 128),
    credential_hash TEXT NOT NULL CHECK(length(credential_hash) = 64),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    expires_at INTEGER NOT NULL CHECK(expires_at > 0)
) STRICT;
CREATE TABLE audit (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL CHECK(timestamp >= 0),
    operator_uid INTEGER NOT NULL CHECK(operator_uid >= 0),
    action TEXT NOT NULL CHECK(action IN ('init', 'register', 'rotate', 'revoke')),
    tether_id TEXT,
    expires_at INTEGER
) STRICT;
PRAGMA user_version = 1;
";

/// A validated durable registration. Debug deliberately omits its digest.
#[derive(Clone, PartialEq, Eq)]
pub struct Record {
    pub id: String,
    pub label: String,
    pub credential_hash: String,
    pub active: bool,
    pub expires_at: u64,
}

impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Record")
            .field("id", &self.id)
            .field("label", &self.label)
            .field("active", &self.active)
            .field("expires_at", &self.expires_at)
            .finish_non_exhaustive()
    }
}

/// One local SQLite connection. Every operation rechecks OS access and identity.
pub struct Store {
    connection: Connection,
    directory: PathBuf,
    directory_identity: (u64, u64),
    database_identity: (u64, u64),
}

impl Store {
    /// Explicitly initialize a new database, refusing to overwrite any path.
    pub fn init(directory: &Path) -> Result<Self, Error> {
        let directory = normalized_directory(directory)?;
        match fs::DirBuilder::new().mode(0o700).create(&directory) {
            Ok(()) => {
                fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
                open_directory(parent_of(&directory))?.sync_all()?;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
        let directory_metadata = private_metadata(&directory, true)?;
        let database = directory.join(DATABASE_NAME);
        let created = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&database)?;
        created.set_permissions(fs::Permissions::from_mode(0o600))?;
        created.sync_all()?;
        open_directory(&directory)?.sync_all()?;
        let initialized = (|| {
            let mut store = Self::connect(directory.clone(), directory_metadata)?;
            let transaction = store
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute_batch(SCHEMA)?;
            append_audit(&transaction, "init", None, None)?;
            transaction.commit()?;
            open_directory(&directory)?.sync_all()?;
            Ok(store)
        })();
        if initialized.is_err() {
            let _ = fs::remove_file(&database);
            let _ = open_directory(&directory).and_then(|parent| Ok(parent.sync_all()?));
        }
        initialized
    }

    /// Open existing, valid state. Missing state is never silently initialized.
    pub fn open(directory: &Path) -> Result<Self, Error> {
        let directory = normalized_directory(directory)?;
        let metadata = private_metadata(&directory, true)?;
        let store = Self::connect(directory, metadata)?;
        check_schema(&store.connection)?;
        let integrity: String = store
            .connection
            .query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
        if integrity != "ok" {
            return Err("database integrity check failed".into());
        }
        store.check_files()?;
        Ok(store)
    }

    fn connect(directory: PathBuf, directory_metadata: Metadata) -> Result<Self, Error> {
        // SQLite NOFOLLOW also rejects symlink ancestors. Resolve only the
        // already-validated directory so normal OS aliases such as macOS /var
        // work, while the database itself is still opened without following it.
        let path = fs::canonicalize(&directory)?.join(DATABASE_NAME);
        let database_metadata = private_metadata(&path, false)?;
        let connection = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        // A read blocked by a writer returns before the hub's two-second watchdog.
        connection.busy_timeout(Duration::from_secs(1))?;
        connection.pragma_update(None, "journal_mode", "DELETE")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", true)?;
        let store = Self {
            connection,
            directory,
            directory_identity: identity(&directory_metadata),
            database_identity: identity(&database_metadata),
        };
        store.check_files()?;
        Ok(store)
    }

    fn check_files(&self) -> Result<(), Error> {
        if identity(&private_metadata(&self.directory, true)?) != self.directory_identity
            || identity(&private_metadata(
                &self.directory.join(DATABASE_NAME),
                false,
            )?) != self.database_identity
        {
            return Err("state directory or database was replaced".into());
        }
        Ok(())
    }

    /// Return an ordered, complete snapshot or fail without returning partial state.
    pub fn snapshot(&self) -> Result<Vec<Record>, Error> {
        self.check_files()?;
        let transaction = self.connection.unchecked_transaction()?;
        check_schema(&transaction)?;
        let mut records = Vec::new();
        {
            let mut statement = transaction.prepare(
                "SELECT id,label,credential_hash,active,expires_at FROM registrations ORDER BY id LIMIT 10001",
            )?;
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                let active: i64 = row.get(3)?;
                let record = Record {
                    id: row.get(0)?,
                    label: row.get(1)?,
                    credential_hash: row.get(2)?,
                    active: active == 1,
                    expires_at: u64::try_from(row.get::<_, i64>(4)?)?,
                };
                if active != 0 && active != 1 {
                    return Err("invalid registration state".into());
                }
                validate_id(&record.id)?;
                validate_label(&record.label)?;
                if record.credential_hash.len() != 64
                    || !record
                        .credential_hash
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                    || record.expires_at == 0
                {
                    return Err("invalid persisted registration".into());
                }
                records.push(record);
                if records.len() > MAX_REGISTRATIONS {
                    return Err("registration limit exceeded".into());
                }
            }
        }
        transaction.commit()?;
        self.check_files()?;
        Ok(records)
    }

    /// Register a unique stable ID and deliver its credential privately.
    pub fn register(
        &mut self,
        id: &str,
        label: &str,
        ttl_secs: u64,
        output: &Path,
    ) -> Result<(), Error> {
        validate_label(label)?;
        self.issue(id, Some(label), ttl_secs, output)
    }

    /// Replace an existing credential and reactivate its registration.
    pub fn rotate(&mut self, id: &str, ttl_secs: u64, output: &Path) -> Result<(), Error> {
        self.issue(id, None, ttl_secs, output)
    }

    fn issue(
        &mut self,
        id: &str,
        label: Option<&str>,
        ttl_secs: u64,
        output: &Path,
    ) -> Result<(), Error> {
        validate_id(id)?;
        if !(60..=86400).contains(&ttl_secs) {
            return Err("credential TTL must be 60 through 86400 seconds".into());
        }
        let expires_at = unix_time()?
            .checked_add(ttl_secs)
            .filter(|time| *time <= i64::MAX as u64)
            .ok_or("credential expiry is out of range")?;
        self.check_files()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        check_schema(&transaction)?;
        let mut random = [0_u8; 32];
        OsRng
            .try_fill_bytes(&mut random)
            .map_err(|_| "OS credential randomness unavailable")?;
        let credential: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
        let credential_hash = format!("{:x}", Sha256::digest(credential.as_bytes()));
        let action = if let Some(label) = label {
            let count: i64 =
                transaction
                    .query_row("SELECT count(*) FROM registrations", [], |row| row.get(0))?;
            if count >= MAX_REGISTRATIONS as i64 {
                return Err("registration limit reached".into());
            }
            transaction.execute(
                "INSERT INTO registrations(id,label,credential_hash,active,expires_at) VALUES (?1,?2,?3,1,?4)",
                params![id, label, credential_hash, expires_at as i64],
            ).map_err(|_| "registration failed; ID must be unique and state must be writable")?;
            "register"
        } else {
            let changed = transaction.execute(
                "UPDATE registrations SET credential_hash=?1, active=1, expires_at=?2 WHERE id=?3",
                params![credential_hash, expires_at as i64, id],
            )?;
            if changed != 1 {
                return Err("registration does not exist".into());
            }
            "rotate"
        };
        append_audit(&transaction, action, Some(id), Some(expires_at))?;
        let mut delivered = PendingCredential::create(output, &credential)?;
        transaction.commit()?;
        delivered.committed = true;
        Ok(())
    }

    /// Disable an active registration. Returns false for absent/already revoked IDs.
    pub fn revoke(&mut self, id: &str) -> Result<bool, Error> {
        validate_id(id)?;
        self.check_files()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        check_schema(&transaction)?;
        let changed = transaction.execute(
            "UPDATE registrations SET active=0 WHERE id=?1 AND active=1",
            [id],
        )?;
        if changed == 1 {
            append_audit(&transaction, "revoke", Some(id), None)?;
        }
        transaction.commit()?;
        Ok(changed == 1)
    }

    fn audit(&self, after: u64) -> Result<Vec<Value>, Error> {
        let after = i64::try_from(after).map_err(|_| "audit sequence is out of range")?;
        self.check_files()?;
        let transaction = self.connection.unchecked_transaction()?;
        check_schema(&transaction)?;
        let events = {
            let mut statement = transaction.prepare(
                "SELECT sequence,timestamp,operator_uid,action,tether_id,expires_at FROM audit WHERE sequence>?1 ORDER BY sequence LIMIT 100",
            )?;
            let events = statement
                .query_map([after], |row| {
                    Ok(json!({
                        "sequence": row.get::<_, i64>(0)?,
                        "timestamp": row.get::<_, i64>(1)?,
                        "operator_uid": row.get::<_, u32>(2)?,
                        "action": row.get::<_, String>(3)?,
                        "tether_id": row.get::<_, Option<String>>(4)?,
                        "expires_at": row.get::<_, Option<i64>>(5)?,
                    }))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            events
        };
        transaction.commit()?;
        self.check_files()?;
        Ok(events)
    }

    fn list(&self) -> Result<Vec<Value>, Error> {
        let now = unix_time()?;
        Ok(self.snapshot()?.into_iter().map(|record| {
            let state = if !record.active { "revoked" } else if record.expires_at <= now { "expired" } else { "active" };
            json!({"id": record.id, "label": record.label, "active": state == "active", "state": state, "expires_at": record.expires_at})
        }).collect())
    }
}

fn check_schema(connection: &Connection) -> Result<(), Error> {
    let version: i64 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version != 1 {
        return Err("unsupported database schema version".into());
    }
    // Preparing both projections detects missing tables/columns even in empty state.
    connection
        .prepare("SELECT id,label,credential_hash,active,expires_at FROM registrations LIMIT 0")?;
    connection.prepare(
        "SELECT sequence,timestamp,operator_uid,action,tether_id,expires_at FROM audit LIMIT 0",
    )?;
    Ok(())
}

fn append_audit(
    transaction: &Transaction<'_>,
    action: &str,
    id: Option<&str>,
    expiry: Option<u64>,
) -> Result<(), Error> {
    transaction.execute(
        "INSERT INTO audit(timestamp,operator_uid,action,tether_id,expires_at) VALUES (?1,?2,?3,?4,?5)",
        params![i64::try_from(unix_time()?)?, effective_uid(), action, id, expiry.map(i64::try_from).transpose()?],
    )?;
    Ok(())
}

fn effective_uid() -> u32 {
    // SAFETY: geteuid has no arguments or memory preconditions.
    unsafe { libc::geteuid() }
}

fn unix_time() -> Result<u64, Error> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

fn validate_id(id: &str) -> Result<(), Error> {
    let valid = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    if id.is_empty()
        || id.len() > 63
        || !valid(id.as_bytes()[0])
        || !id.bytes().all(|byte| valid(byte) || byte == b'-')
    {
        return Err("ID must contain 1 through 63 lowercase ASCII letters, digits or hyphens, starting with a letter or digit".into());
    }
    Ok(())
}

fn validate_label(label: &str) -> Result<(), Error> {
    if label.is_empty() || label.chars().count() > 128 || label.chars().any(char::is_control) {
        return Err("label must contain 1 through 128 characters without controls".into());
    }
    Ok(())
}

fn normalized_directory(directory: &Path) -> Result<PathBuf, Error> {
    if directory.as_os_str().is_empty() {
        return Err("state directory must not be empty".into());
    }
    // Removing a terminal `.` also prevents `symlink/.` bypassing lstat.
    let path: PathBuf = directory.components().collect();
    Ok(path)
}

fn parent_of(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

fn open_directory(path: &Path) -> Result<File, Error> {
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(path)?)
}

fn identity(metadata: &Metadata) -> (u64, u64) {
    (metadata.dev(), metadata.ino())
}

fn private_metadata(path: &Path, directory: bool) -> Result<Metadata, Error> {
    let metadata = fs::symlink_metadata(path)?;
    let expected_mode = if directory { 0o700 } else { 0o600 };
    if metadata.uid() != effective_uid()
        || metadata.mode() & 0o7777 != expected_mode
        || (directory && !metadata.is_dir())
        || (!directory && (!metadata.is_file() || metadata.nlink() != 1))
    {
        return Err(if directory {
            "state directory must be owned by the effective UID, mode 0700, and not a symlink"
        } else {
            "database must be a regular, singly linked, non-symlink file owned by the effective UID with mode 0600"
        }.into());
    }
    Ok(metadata)
}

/// Removes a freshly created file on every failure before commit, including
/// write/sync failures. Output and parent are already durable when create returns.
struct PendingCredential {
    path: PathBuf,
    parent: File,
    committed: bool,
}

impl PendingCredential {
    fn create(path: &Path, credential: &str) -> Result<Self, Error> {
        let parent = open_directory(parent_of(path))?;
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        let pending = Self {
            path: path.to_owned(),
            parent,
            committed: false,
        };
        output.set_permissions(fs::Permissions::from_mode(0o600))?;
        writeln!(output, "{credential}")?;
        output.sync_all()?;
        pending.parent.sync_all()?;
        Ok(pending)
    }
}

impl Drop for PendingCredential {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.path);
            let _ = self.parent.sync_all();
        }
    }
}

/// Run arguments following `anvil-ring admin`; output contains no credentials.
pub fn run(args: &[String]) -> Result<(), Error> {
    let directory = std::env::var_os("ANVIL_RING_STATE_DIR")
        .filter(|v| !v.is_empty())
        .ok_or("ANVIL_RING_STATE_DIR is required")?;
    let directory = Path::new(&directory);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let output_path = || -> Result<PathBuf, Error> {
        Ok(std::env::var_os("ANVIL_RING_CREDENTIAL_OUT")
            .filter(|v| !v.is_empty())
            .ok_or("ANVIL_RING_CREDENTIAL_OUT is required for issuance")?
            .into())
    };
    let result = match args.as_slice() {
        ["init"] => {
            Store::init(directory)?;
            json!({"action": "init", "ok": true})
        }
        ["register", id, label, ttl] => {
            Store::open(directory)?.register(id, label, ttl.parse().map_err(|_| "TTL must be an integer")?, &output_path()?)?;
            json!({"action": "register", "id": id, "ok": true})
        }
        ["rotate", id, ttl] => {
            Store::open(directory)?.rotate(id, ttl.parse().map_err(|_| "TTL must be an integer")?, &output_path()?)?;
            json!({"action": "rotate", "id": id, "ok": true})
        }
        ["revoke", id] => {
            let changed = Store::open(directory)?.revoke(id)?;
            json!({"action": "revoke", "id": id, "changed": changed})
        }
        ["list"] => json!(Store::open(directory)?.list()?),
        ["audit"] => json!(Store::open(directory)?.audit(0)?),
        ["audit", after] => json!(Store::open(directory)?.audit(after.parse().map_err(|_| "audit sequence must be a nonnegative integer")?)?),
        _ => return Err("usage: admin init | register ID LABEL TTL_SECONDS | rotate ID TTL_SECONDS | revoke ID | list | audit [AFTER_SEQUENCE]".into()),
    };
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, &result)?;
    writeln!(stdout)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::fs;
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    use std::sync::{Arc, Barrier};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "anvil-admin-{}-{:016x}",
                std::process::id(),
                rand::random::<u64>()
            ));
            fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
            Self(path)
        }
        fn state(&self) -> PathBuf {
            self.0.join("state")
        }
        fn token(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[test]
    fn persistence_rotation_revocation_and_expiry_metadata() {
        let temp = Temp::new();
        let mut store = Store::init(&temp.state()).unwrap();
        let before = now();
        store
            .register("a-1", "Rental A", 60, &temp.token("first"))
            .unwrap();
        let first = store.snapshot().unwrap().remove(0);
        assert_eq!(first.id, "a-1");
        assert_eq!(first.label, "Rental A");
        assert!(first.active);
        assert!((before + 60..=now() + 60).contains(&first.expires_at));
        let credential = fs::read_to_string(temp.token("first")).unwrap();
        assert_eq!(credential.trim().len(), 64);
        assert!(credential.trim().bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(
            first.credential_hash,
            format!("{:x}", Sha256::digest(credential.trim().as_bytes()))
        );
        assert!(!format!("{first:?}").contains(&first.credential_hash));
        drop(store);
        let mut store = Store::open(&temp.state()).unwrap();
        assert_eq!(store.snapshot().unwrap(), vec![first.clone()]);
        store.rotate("a-1", 86400, &temp.token("second")).unwrap();
        let second = store.snapshot().unwrap().remove(0);
        assert_ne!(second.credential_hash, first.credential_hash);
        assert!(second.expires_at >= before + 86400);
        assert!(store.revoke("a-1").unwrap());
        assert!(!store.revoke("a-1").unwrap());
        assert!(!store.revoke("missing").unwrap());
        drop(store);
        let mut store = Store::open(&temp.state()).unwrap();
        assert!(!store.snapshot().unwrap()[0].active);
        store.rotate("a-1", 60, &temp.token("third")).unwrap();
        assert!(store.snapshot().unwrap()[0].active);
        let audit = store.audit(0).unwrap();
        assert_eq!(
            audit
                .iter()
                .map(|row| row["action"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["init", "register", "rotate", "revoke", "rotate"]
        );
        assert!(audit
            .iter()
            .all(|row| row["operator_uid"].as_u64()
                == Some(fs::metadata(&temp.0).unwrap().uid() as u64)));
        assert_eq!(audit[1]["expires_at"].as_u64(), Some(first.expires_at));
    }

    #[test]
    fn private_files_and_initialization_are_explicit() {
        let temp = Temp::new();
        assert!(Store::open(&temp.state()).is_err());
        assert!(!temp.state().exists());
        let mut store = Store::init(&temp.state()).unwrap();
        store
            .register("a", "A", 60, &temp.token("credential"))
            .unwrap();
        for (path, mode) in [
            (temp.state(), 0o700),
            (temp.state().join("hub.sqlite3"), 0o600),
            (temp.token("credential"), 0o600),
        ] {
            assert_eq!(fs::metadata(path).unwrap().mode() & 0o7777, mode);
        }
        assert!(Store::init(&temp.state()).is_err());
        assert_eq!(store.snapshot().unwrap().len(), 1);
        let journal: String = store
            .connection
            .pragma_query_value(None, "journal_mode", |r| r.get(0))
            .unwrap();
        let sync: i64 = store
            .connection
            .pragma_query_value(None, "synchronous", |r| r.get(0))
            .unwrap();
        assert_eq!(journal, "delete");
        assert_eq!(sync, 2);
    }

    #[test]
    fn duplicate_registration_and_output_cannot_change_persisted_state() {
        let temp = Temp::new();
        let mut store = Store::init(&temp.state()).unwrap();
        store.register("a", "A", 60, &temp.token("one")).unwrap();
        let original = store.snapshot().unwrap();
        let token = fs::read(temp.token("one")).unwrap();
        assert!(store
            .register("a", "Duplicate", 60, &temp.token("two"))
            .is_err());
        assert!(!temp.token("two").exists());
        assert!(store.register("b", "B", 60, &temp.token("one")).is_err());
        assert!(store.rotate("a", 60, &temp.token("one")).is_err());
        assert!(store.rotate("missing", 60, &temp.token("two")).is_err());
        assert_eq!(fs::read(temp.token("one")).unwrap(), token);
        assert_eq!(store.snapshot().unwrap(), original);
        assert_eq!(store.audit(0).unwrap().len(), 2);
    }

    #[test]
    fn failed_output_and_failed_commit_roll_back_registration_and_audit() {
        let temp = Temp::new();
        let mut store = Store::init(&temp.state()).unwrap();
        assert!(store
            .register("a", "A", 60, &temp.token("missing/credential"))
            .is_err());
        assert!(store.snapshot().unwrap().is_empty());
        assert_eq!(store.audit(0).unwrap().len(), 1);
        // A real deferred FK violation fires only at commit, after the output
        // has been written and synchronized; no test hook changes production.
        store.connection.execute_batch("PRAGMA foreign_keys=ON; CREATE TABLE absent(id INTEGER PRIMARY KEY); CREATE TABLE commit_failure(id INTEGER REFERENCES absent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fail_commit AFTER INSERT ON registrations BEGIN INSERT INTO commit_failure VALUES (1); END;").unwrap();
        assert!(store.register("a", "A", 60, &temp.token("failed")).is_err());
        assert!(!temp.token("failed").exists());
        assert!(store.snapshot().unwrap().is_empty());
        assert_eq!(store.audit(0).unwrap().len(), 1);
    }

    #[test]
    fn persisted_database_and_audit_never_contain_plaintext_credentials() {
        let temp = Temp::new();
        let mut store = Store::init(&temp.state()).unwrap();
        store.register("a", "A", 60, &temp.token("one")).unwrap();
        store.rotate("a", 60, &temp.token("two")).unwrap();
        let output =
            serde_json::to_string(&(store.list().unwrap(), store.audit(0).unwrap())).unwrap();
        let digest = store.snapshot().unwrap()[0].credential_hash.clone();
        assert!(!output.contains(&digest));
        drop(store);
        for credential_path in [temp.token("one"), temp.token("two")] {
            let secret = fs::read_to_string(credential_path).unwrap();
            assert!(!output.contains(secret.trim()));
            for entry in fs::read_dir(temp.state()).unwrap() {
                let bytes = fs::read(entry.unwrap().path()).unwrap();
                assert!(!bytes
                    .windows(secret.trim().len())
                    .any(|w| w == secret.trim().as_bytes()));
            }
        }
    }

    #[test]
    fn bad_permissions_symlinks_and_disappearing_state_fail_closed() {
        let temp = Temp::new();
        let store = Store::init(&temp.state()).unwrap();
        fs::set_permissions(temp.state(), fs::Permissions::from_mode(0o750)).unwrap();
        assert!(Store::open(&temp.state()).is_err());
        assert!(store.snapshot().is_err());
        fs::set_permissions(temp.state(), fs::Permissions::from_mode(0o700)).unwrap();
        let database = temp.state().join("hub.sqlite3");
        fs::set_permissions(&database, fs::Permissions::from_mode(0o640)).unwrap();
        assert!(Store::open(&temp.state()).is_err());
        assert!(store.snapshot().is_err());
        fs::set_permissions(&database, fs::Permissions::from_mode(0o600)).unwrap();
        let link = temp.token("state-link");
        std::os::unix::fs::symlink(temp.state(), &link).unwrap();
        assert!(Store::open(&link).is_err());
        assert!(Store::open(&link.join(".")).is_err());
        assert!(Store::init(&link).is_err());
        let moved = temp.token("moved.sqlite3");
        fs::rename(&database, &moved).unwrap();
        assert!(store.snapshot().is_err());
        std::os::unix::fs::symlink(&moved, &database).unwrap();
        assert!(Store::open(&temp.state()).is_err());
        assert!(store.snapshot().is_err());
    }

    #[test]
    fn unknown_schema_corrupt_database_and_invalid_records_are_rejected() {
        let temp = Temp::new();
        let store = Store::init(&temp.state()).unwrap();
        store
            .connection
            .pragma_update(None, "user_version", 2)
            .unwrap();
        assert!(Store::open(&temp.state()).is_err());
        assert!(store.snapshot().is_err());
        store
            .connection
            .pragma_update(None, "user_version", 1)
            .unwrap();
        store.connection.execute_batch("PRAGMA ignore_check_constraints=ON; INSERT INTO registrations(id,label,credential_hash,active,expires_at) VALUES ('UPPER','bad','abc',1,1);").unwrap();
        assert!(store.snapshot().is_err());
        drop(store);
        fs::write(temp.state().join("hub.sqlite3"), b"not sqlite").unwrap();
        assert!(Store::open(&temp.state()).is_err());
    }

    #[test]
    fn replaced_database_invalidates_an_already_open_store() {
        let temp = Temp::new();
        let store = Store::init(&temp.state()).unwrap();
        let path = temp.state().join("hub.sqlite3");
        let replacement = temp.token("replacement.sqlite3");
        fs::copy(&path, &replacement).unwrap();
        fs::rename(&replacement, &path).unwrap();
        assert!(store.snapshot().is_err());
        assert!(Store::open(&temp.state())
            .unwrap()
            .snapshot()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn missing_schema_columns_fail_even_when_no_records_exist() {
        let temp = Temp::new();
        let store = Store::init(&temp.state()).unwrap();
        store
            .connection
            .execute_batch("DROP TABLE registrations; CREATE TABLE registrations(id TEXT);")
            .unwrap();
        assert!(store.snapshot().is_err());
        assert!(Store::open(&temp.state()).is_err());
    }

    #[test]
    fn rejects_invalid_ids_labels_ttls_without_creating_a_credential() {
        let temp = Temp::new();
        let mut store = Store::init(&temp.state()).unwrap();
        let output = temp.token("secret");
        for id in [
            "",
            "Upper",
            "-first",
            "a_b",
            "a b",
            "a/b",
            "a\nb",
            &"a".repeat(64),
        ] {
            assert!(
                store.register(id, "A", 60, &output).is_err(),
                "accepted {id:?}"
            );
        }
        for label in ["", "new\nline", "a\0b", &"a".repeat(129)] {
            assert!(store.register("a", label, 60, &output).is_err());
        }
        for ttl in [0, 59, 86401, u64::MAX] {
            assert!(store.register("a", "A", ttl, &output).is_err());
        }
        assert!(!output.exists());
        assert!(store.snapshot().unwrap().is_empty());
        assert_eq!(store.audit(0).unwrap().len(), 1);
        store
            .register(&"a".repeat(63), &"é".repeat(128), 60, &output)
            .unwrap();
    }

    #[test]
    fn list_distinguishes_active_expired_and_revoked_without_hashes() {
        let temp = Temp::new();
        let mut store = Store::init(&temp.state()).unwrap();
        for id in ["active", "expired", "revoked"] {
            store.register(id, id, 60, &temp.token(id)).unwrap();
        }
        store
            .connection
            .execute(
                "UPDATE registrations SET expires_at=1 WHERE id='expired'",
                [],
            )
            .unwrap();
        store.revoke("revoked").unwrap();
        let list = store.list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0]["state"], "active");
        assert_eq!(list[1]["state"], "expired");
        assert_eq!(list[2]["state"], "revoked");
        assert_eq!(list[0]["active"], true);
        assert_eq!(list[1]["active"], false);
        assert_eq!(list[2]["active"], false);
        assert!(list.iter().all(|r| r.get("credential_hash").is_none()));
    }

    #[test]
    fn concurrent_writers_preserve_records_and_exactly_one_audit_per_mutation() {
        let temp = Temp::new();
        drop(Store::init(&temp.state()).unwrap());
        let barrier = Arc::new(Barrier::new(5));
        std::thread::scope(|scope| {
            for index in 0..4 {
                let state = temp.state();
                let output = temp.token(&format!("token-{index}"));
                let barrier = barrier.clone();
                scope.spawn(move || {
                    let mut store = Store::open(&state).unwrap();
                    barrier.wait();
                    store
                        .register(&format!("tether-{index}"), "A", 60, &output)
                        .unwrap();
                });
            }
            barrier.wait();
        });
        let store = Store::open(&temp.state()).unwrap();
        assert_eq!(store.snapshot().unwrap().len(), 4);
        assert_eq!(store.audit(0).unwrap().len(), 5);
    }

    #[test]
    fn competing_duplicate_writers_issue_only_one_credential() {
        let temp = Temp::new();
        drop(Store::init(&temp.state()).unwrap());
        let barrier = Arc::new(Barrier::new(3));
        let outcomes = std::thread::scope(|scope| {
            let mut threads = Vec::new();
            for index in 0..2 {
                let state = temp.state();
                let output = temp.token(&format!("token-{index}"));
                let barrier = barrier.clone();
                threads.push(scope.spawn(move || {
                    let mut store = Store::open(&state).unwrap();
                    barrier.wait();
                    store.register("same-id", "A", 60, &output).is_ok()
                }));
            }
            barrier.wait();
            threads
                .into_iter()
                .map(|t| t.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(outcomes.iter().filter(|&&ok| ok).count(), 1);
        assert_eq!(
            (0..2)
                .filter(|i| temp.token(&format!("token-{i}")).exists())
                .count(),
            1
        );
        let store = Store::open(&temp.state()).unwrap();
        assert_eq!(store.snapshot().unwrap().len(), 1);
        assert_eq!(store.audit(0).unwrap().len(), 2);
    }

    #[test]
    fn credential_output_rejects_symlinks_including_dangling_ones() {
        let temp = Temp::new();
        let mut store = Store::init(&temp.state()).unwrap();
        let output = temp.token("link");
        let target = temp.token("target");
        std::os::unix::fs::symlink(&target, &output).unwrap();
        assert!(store.register("a", "A", 60, &output).is_err());
        assert!(!target.exists());
        fs::write(&target, b"keep").unwrap();
        assert!(store.register("a", "A", 60, &output).is_err());
        assert_eq!(fs::read(target).unwrap(), b"keep");
        assert!(store.snapshot().unwrap().is_empty());
    }

    #[test]
    fn audit_pagination_never_repeats_or_skips_committed_events() {
        let temp = Temp::new();
        let mut store = Store::init(&temp.state()).unwrap();
        for index in 0..101 {
            store
                .register(
                    &format!("tether-{index}"),
                    "A",
                    60,
                    &temp.token(&format!("token-{index}")),
                )
                .unwrap();
        }
        let first = store.audit(0).unwrap();
        assert_eq!(first.len(), 100);
        assert_eq!(first.first().unwrap()["sequence"], 1);
        assert_eq!(first.last().unwrap()["sequence"], 100);
        let second = store.audit(100).unwrap();
        assert_eq!(second.len(), 2);
        assert_eq!(second[0]["sequence"], 101);
        assert_eq!(second[1]["sequence"], 102);
        assert!(store.audit(102).unwrap().is_empty());
        assert!(store.audit(u64::MAX).is_err());
    }

    #[test]
    fn registrations_are_bounded_and_oversized_snapshots_fail_closed() {
        let temp = Temp::new();
        let mut store = Store::init(&temp.state()).unwrap();
        store.connection.execute_batch("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) INSERT INTO registrations(id,label,credential_hash,active,expires_at) SELECT 't-'||x,'A','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',1,9999999999 FROM n;").unwrap();
        assert_eq!(store.snapshot().unwrap().len(), 10000);
        assert!(store
            .register("extra", "A", 60, &temp.token("secret"))
            .is_err());
        assert!(!temp.token("secret").exists());
        store.connection.execute_batch("INSERT INTO registrations VALUES ('extra','A','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',1,9999999999);").unwrap();
        assert!(store.snapshot().is_err());
    }
}
