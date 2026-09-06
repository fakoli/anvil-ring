# ADR-0007: Durable hub administration and routing

Status: implemented and locally verified, 2026-09-05.

## Requirement

Complete the persistent registration, credential lifecycle, authenticated
operator command, audit, and deterministic multi-tether routing requirements in
`STATE.md`. Preserve the outbound-only rental, one static binary, Rust 1.85,
loopback forwarding, independent caller authentication, and streaming guarantees.

## Decision

Use SQLite on the hub's local filesystem and a local `anvil-ring admin` command.
The operating system authenticates the operator account: the state directory
must be owned by the effective UID with mode 0700, and its regular database file
must be private. Symlink state directories and database files are rejected.
Remote operators use their existing authenticated host access to run the command.
No additional network administration listener is required.

SQLite transactions commit registration changes and audit events together.
SQLite is compiled into the binary, preserving the rental artifact contract.
The database uses a rollback journal and FULL synchronization on local disk.
Database corruption, missing state, and unknown schema versions fail closed.
Initialization is explicit; the hub never silently creates an empty replacement.

Alternatives considered: an atomically replaced JSON file would require a custom
transaction and writer-lock protocol for audit and concurrent CLI operations;
a network administration API would require another listener and credential
distribution path. SQLite plus the OS-authenticated command implements the
required control without either additional protocol.

## Credentials and commands

`ANVIL_RING_STATE_DIR` selects the database directory for both hub and admin.

| Command | Behavior |
|---|---|
| `admin init` | Create the private state directory/database with schema version 1 |
| `admin register ID LABEL TTL_SECONDS` | Register a unique stable ID and issue a random credential |
| `admin rotate ID TTL_SECONDS` | Replace the credential for an existing registration and activate it |
| `admin revoke ID` | Persistently disable the registration |
| `admin list` | Report records and effective active/expired/revoked state without credential material |
| `admin audit [AFTER_SEQUENCE]` | Read up to 100 ordered audit events after a sequence number |

Registration IDs use lowercase ASCII letters, digits, and hyphens, with at most
63 characters and a letter or digit first. Labels are nonempty, at most 128
characters, and contain no control characters. Credential TTL is 60–86400
seconds. Credentials contain 256 random bits and only their SHA-256 digest is
stored in SQLite. The expiry timestamp is absolute Unix time; operating clocks
must be synchronized.

Issuance and rotation require `ANVIL_RING_CREDENTIAL_OUT`. The command creates
that file with mode 0600, refuses to overwrite an existing path, and synchronizes
the file and its parent before committing the database transaction. It never
prints the token. A failure before database commit removes the new output file.
A crash before commit can leave an unregistered credential file; a committed
credential has already been delivered durably. Retry with a new file if the
outcome was uncertain, checking list/audit first.

Audit events include sequence, timestamp, effective UID, action, tether ID, and
expiry where applicable, without credentials or digests. They are transactionally
consistent operational history; the database owner can alter the database, so
tamper resistance requires export to a separate audit sink.

## Applying control changes

The hub loads the complete validated registration snapshot before accepting
connections, then refreshes it every 500 ms in a blocking worker. Normal
administrative changes take effect at the next refresh. A read failure clears
authorization and cancels live sessions. A read exceeding two seconds does the
same; the monitor waits for that worker to finish before trying again, preventing
an accumulation of blocked workers.

Applying a snapshot is atomic with session attachment and routing checks.
Unchanged registrations keep their sessions. Removed, revoked, rotated, or
expired registrations cancel their sessions and fail their in-flight requests.
Authorization and routing also check expiry directly; issued leases cannot
outlive the credential. Reconnection requires the current credential.

`ANVIL_RING_DEMO_CREDENTIAL` remains an explicit local/pilot compatibility mode.
Setting it together with durable state is an error. It is never imported
implicitly into durable state.

## Routing and admission

All registrations in one hub belong to one serving pool and must expose the
same intended serving contract. Model lifecycle and capability selection remain
with the serving upstream. Callers and tethers cannot choose a rental ID.

For each request the hub considers active, unexpired, responsive sessions with
stream and command capacity. It selects the fewest active streams, breaking ties
by registration ID in lexical order. Selection and admission are serialized, and
an OPEN queue slot is reserved before sending any request bytes. If all eligible
sessions are full, return 503; if none is connected, return 502. Requests are not
retried on a different engine after OPEN, avoiding duplicate model work.

## Verification evidence

Tests exercise persistence across process restarts; duplicate registration;
private state and credential files; concurrent writers; transaction/audit
consistency; rotation, revocation, and expiry on a live tunnel; store failure;
deterministic routing and saturation across two real tethers; and the existing
streaming/failure contracts. The complete 149-test Rust suite passes on macOS
(stable and Rust 1.85) and both Linux musl architectures (Rust 1.85). Both rebuilt
static binaries pass checksum and unprivileged SQLite administration checks.
See [Testing](../testing.md) for commands and coverage limits. Credential delivery
is verified by transaction-failure injection and synchronization ordering;
physical power-loss durability has not been tested.

This decision does not substitute for the remaining deployment gates: trusted
TLS/certificate rotation, provider egress, a real GPU/model container, a hosted
signed release, publication, and measured operating objectives.

References: [SQLite atomic commit](https://www.sqlite.org/atomiccommit.html),
[SQLite synchronization](https://www.sqlite.org/pragma.html#pragma_synchronous),
[rusqlite bundled linkage](https://github.com/rusqlite/rusqlite).
