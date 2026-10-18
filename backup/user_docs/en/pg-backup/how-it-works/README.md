# How it works

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
