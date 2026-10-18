# Restoring a backup

Restoring is done out of the admin, by whoever administers the database
server, with the tools of PostgreSQL (`pg_restore`, of the version of the
server or newer).

1. **Download** the backup, on the page of the database: a `.tar` file.
2. **Take the copy out** of it:

   ```sh
   tar -xf 20261018T030000.tar backup.pg
   ```

3. **Look at it** before restoring — the list of what it has:

   ```sh
   pg_restore --list backup.pg | head
   ```

4. **Restore it into a new database**, and check it there:

   ```sh
   createdb restored
   pg_restore --no-owner --no-privileges -d restored backup.pg
   ```

## Over the database in use

Restoring over the database the application uses **replaces its data**:

- make a backup of it first (Create backup), and download it;
- stop the application while restoring;
- restore with `--clean --if-exists`, which drops what is there before:

  ```sh
  pg_restore --clean --if-exists --no-owner --no-privileges -d <database> backup.pg
  ```

- start the application again, and check the site and the admin.
