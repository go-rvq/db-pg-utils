# When a backup fails

A failed automatic backup is a job with **Exception** (Jobs): its detail has
the log and the error. A failed **Create backup** says the error at once.

What the application finds when it starts is on the page of the database:
a method that may not be used is offered as **not available**, with why — the
PL/Python of a server that cannot write its `/tmp` says, for instance,
*The database server cannot make the copy (pg_dump: error: could not open
output file "/tmp/…": Permission denied)*.

| The error says | What happened | What to do |
|---|---|---|
| `language "plpython3u" does not exist` | The database has no PL/Python. | Install it on the server and create the extension (see Requirements), or keep the backups off. |
| `must be superuser to create … function` | The function of an untrusted language is created by a superuser. | Create it once as a superuser, or give the application's user that right. |
| `Connection refused`, a timeout | The database server does not reach the application on its port. | Put both on the same network; open the port between them. |
| An error of `pg_dump` | The copy failed on the server (a permission, the disk). | Its message says which; look at the server. |
| `No space left on device` | No room in `/tmp` of the server, or in the data directory of the application. | Free room; lower the persistence. |
| `PL/Python: pg_dump: error: could not open output file "/tmp/…": Permission denied` | The `pg_dump` of the database server cannot write in its `/tmp`: not writable by the system user `postgres` — in a container, a `/tmp` mounted from the server with `755` of `root`. | Make that directory `1777` (`chmod 1777 <it>`, on the server), or use the method pg_dump, by the application. |
| `error fetching next item from iterator` | Versions before October 2026 said only this of an error in PL/Python: what happened was in its detail. | Update the application, which says it; or see it as below. |
| `pg_dump: … server version mismatch` | The `pg_dump` of the application is older than the database server. | Install the `pg_dump` of the server's version (or newer), or point `DB_PG_DUMP` to it. |
| `pg_dump: … connection … failed` | The method pg_dump could not connect as the application does. | The message says why (the host, the password); the application itself connects with the same. |
| `backup method not available` | The method chosen may not be used now. | The page of the database says why; choose the other, or fix what it says. |
| `file does not exist` | Versions before the fix of October 2026 could not find the file they had just made — the backup was made, the job said it failed, and the automatic ones were kept as manual. | Update the application. The automatic backups kept as manual are not removed by the persistence: remove them by hand. |

## Seeing the error of PL/Python

The function may be called by hand, where `psql` shows all of the error (its
`DETAIL`) — on the database server, as the user of the application, with the
address and the port the application listens on for the backups (in its log:
`db-pg-utils:storage_server … Listening on port`):

```sh
psql -U <user> <database> -c "SELECT * FROM db_backup__create('test', '<address of the application>:<port>', 'auto=false')"
```

When it works, it makes a manual backup (the note "test"), listed on the
page of the database.
