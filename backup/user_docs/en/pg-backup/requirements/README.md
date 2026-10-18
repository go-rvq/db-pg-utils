# Requirements

## On the database server

- **PL/Python** (`plpython3u`): the language of the function that makes the
  backup. Its package is installed on the server (on Debian,
  `postgresql-plpython3-<version>`), and the language created in the database
  (`CREATE EXTENSION plpython3u`).
- The user of the application creates the function when it starts: a function
  of an untrusted language (`plpython3u`) is created by a **superuser** only.
- `pg_dump` on the server — it comes with PostgreSQL, of its version.
- **Room in `/tmp`** of the server for one copy while it is sent.

## Between the two

- The database server **reaches the application**: the address it sees the
  application at, on the port the application listens on for the backups
  (a free port, chosen when it starts — in the log:
  `db-pg-utils:storage_server … Listening on port`). In containers, both on
  the same network; no firewall between them.

## On the application

- **Room in its data directory** (`data/db-backup`) for the backups the
  persistence keeps.
- A data directory that survives a new version of the application (a
  volume).

Without PL/Python on the database, the application does not start its
backups: it is configured with them off (the administrator of the server
decides it).
