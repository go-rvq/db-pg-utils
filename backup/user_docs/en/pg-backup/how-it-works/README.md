# How it works

A backup is made by one of two **methods**, chosen on the page of the
database (**Backup method**, beside the persistence). Both make the same file.

## PL/Python, by the database server

The copy is made by **the database server itself**, and sent to the
application:

1. The application asks the database for a backup: it calls the function
   `db_backup__create`, which it installs in the database when it starts.
2. The function, written in PL/Python (`plpython3u`), runs `pg_dump` on the
   database server — the copy of the database, in PostgreSQL's custom format,
   with no owners nor privileges (`pg_dump -xO -Fc`).
3. It sends the file to the application by HTTP: the application listens on
   a port of its own (chosen when it starts), at the address the database
   sees it at.
4. The application keeps the file, and the job says what was made.

## pg_dump, by the application

The copy is made by **the application**, with the connection it already
uses — nothing installed in the database, no port between them:

1. The application runs `pg_dump -xO -Fc` (the one of `DB_PG_DUMP`, else
   `pg_dump` in its PATH), connected as it is connected: the host, the port,
   the user, the password and the database of its connection go to `pg_dump`
   by its environment (`PGHOST`, `PGPASSWORD`, …) — the password is never on
   a command line.
2. It keeps the file, and the job says what was made. Stopping the job stops
   `pg_dump`.

When the application starts, it checks which methods may be used: PL/Python
when its function could be created **and the database server can make the
copy** — its `pg_dump` there, its `/tmp` writable (a test file, written and
removed) —, pg_dump when it is there and **not older than the database
server**. The page says why one may not. With none chosen,
the backups use the first one that may be used, PL/Python first.

## The files

The backups are kept in the data directory of the application:

```
data/db-backup/
  auto/<database>/<year>/<month>/<day>/<YYYYMMDDTHHMMSS>.tar
  manual/<database>/<year>/<month>/<day>/<YYYYMMDDTHHMMSS>.tar
```

The name is the moment of the backup, in UTC. Each file is a `tar` with:

- `message.txt` — the note of the backup ("auto backup" for the automatic
  ones);
- `backup.pg` — the copy of the database (`pg_dump` custom format), what
  `pg_restore` reads.

Whatever keeps the data directory keeps the backups: on a server, the
directory must be on a volume that survives a new version of the
application.
