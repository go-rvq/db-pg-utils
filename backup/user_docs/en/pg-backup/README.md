# Backups of the database

The backups of the PostgreSQL database: a copy of all its data, made once a
day (the **automatic backup**) or when someone asks (**Create backup**, on the
page of the database) — by the database server (PL/Python) or by the
application (pg_dump), the **backup method** chosen there.

Each backup is a file kept by the application, listed on the page of the
database, where it may be downloaded or removed.

- [How it works](how-it-works/README.md) — who makes the copy, where it goes,
  what is in the file.
- [Requirements](requirements/README.md) — what the database server and the
  application need.
- [Restoring a backup](restore/README.md) — from the downloaded file to a
  database.
- [When a backup fails](troubleshooting/README.md) — the errors and what to do.

## Automatic and manual

- The **automatic backup** runs once a day, as a job (Jobs). After it, the
  automatic backups older than the **persistence** allows are removed: so many
  days, weeks, months, years.
- A **manual backup** (Create backup) has a note, written by whoever made it.
  It is never removed by the persistence: only by hand.
