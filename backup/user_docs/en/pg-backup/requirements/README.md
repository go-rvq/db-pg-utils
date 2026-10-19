# Requirements

One method that may be used is enough — the page of the database says which
may, and why not.

## PL/Python, by the database server

- **PL/Python** (`plpython3u`) on the database server: its package installed
  (on Debian, `postgresql-plpython3-<version>`), and the language created in
  the database (`CREATE EXTENSION plpython3u`).
- The user of the application creates the function when it starts: a function
  of an untrusted language (`plpython3u`) is created by a **superuser** only.
- `pg_dump` on the server — it comes with PostgreSQL, of its version.
- **Room in `/tmp`** of the server for one copy while it is sent.
- The database server **reaches the application**: the address it sees the
  application at, on the port the application listens on for the backups
  (a free port, chosen when it starts — in the log:
  `db-pg-utils:storage_server … Listening on port`). In containers, both on
  the same network; no firewall between them.

## pg_dump, by the application

- **`pg_dump` where the application runs**, of the major version of the
  database server **or newer** — an older one refuses to back it up. On
  Debian, the package `postgresql-client-<version>` (for a version newer than
  the distribution's, from the repository of PostgreSQL, apt.postgresql.org).
- Where it is: `DB_PG_DUMP` (in the `env:` of the configuration), a path or a
  name in the PATH — `pg_dump` when not set.
- The user of the application may read all of the database (what it backs
  up).

## On the application, for both

- **Room in its data directory** (`data/db-backup`) for the backups the
  persistence keeps.
- A data directory that survives a new version of the application (a
  volume).
