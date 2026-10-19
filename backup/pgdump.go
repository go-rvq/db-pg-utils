package backup

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	db_tools "github.com/go-rvq/rvq/x/packages/db-tools"
	"gorm.io/driver/postgres"
)

// pgEnv are the libpq keywords of a connection string that pg_dump takes,
// by the variable it reads them from: the others (TimeZone, of the driver)
// are not libpq's.
var pgEnv = map[string]string{
	"host":                 "PGHOST",
	"hostaddr":             "PGHOSTADDR",
	"port":                 "PGPORT",
	"user":                 "PGUSER",
	"password":             "PGPASSWORD",
	"dbname":               "PGDATABASE",
	"sslmode":              "PGSSLMODE",
	"sslrootcert":          "PGSSLROOTCERT",
	"sslcert":              "PGSSLCERT",
	"sslkey":               "PGSSLKEY",
	"connect_timeout":      "PGCONNECT_TIMEOUT",
	"application_name":     "PGAPPNAME",
	"target_session_attrs": "PGTARGETSESSIONATTRS",
}

// ConnEnv are the PG* variables of the connection string dsn — keyword=value
// (libpq's, values quoted with '…' as libpq quotes them) or a URL
// (postgres://user:password@host:port/dbname?sslmode=…): what pg_dump reads,
// so the password is never on its command line.
func ConnEnv(dsn string) (env []string, err error) {
	var kv map[string]string
	if kv, err = parseConn(dsn); err != nil {
		return
	}
	for k, v := range kv {
		if name, ok := pgEnv[strings.ToLower(k)]; ok {
			env = append(env, name+"="+v)
		}
	}
	return
}

func parseConn(dsn string) (map[string]string, error) {
	dsn = strings.TrimSpace(dsn)
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return nil, fmt.Errorf("the connection URL: %w", errors.Unwrap(err))
		}
		kv := map[string]string{}
		if u.User != nil {
			kv["user"] = u.User.Username()
			if p, ok := u.User.Password(); ok {
				kv["password"] = p
			}
		}
		if h := u.Hostname(); h != "" {
			kv["host"] = h
		}
		if p := u.Port(); p != "" {
			kv["port"] = p
		}
		if db := strings.TrimPrefix(u.Path, "/"); db != "" {
			kv["dbname"] = db
		}
		for k, vs := range u.Query() {
			if len(vs) > 0 {
				kv[k] = vs[len(vs)-1]
			}
		}
		return kv, nil
	}
	return parseKeywords(dsn)
}

// parseKeywords reads keyword=value pairs as libpq does: spaces around the
// "=", a value in single quotes, \' and \\ escaped in it.
func parseKeywords(s string) (map[string]string, error) {
	kv := map[string]string{}
	i, n := 0, len(s)
	for {
		for i < n && s[i] == ' ' {
			i++
		}
		if i >= n {
			return kv, nil
		}
		start := i
		for i < n && s[i] != '=' && s[i] != ' ' {
			i++
		}
		key := s[start:i]
		for i < n && s[i] == ' ' {
			i++
		}
		if i >= n || s[i] != '=' {
			return nil, fmt.Errorf("the connection string: %q has no value", key)
		}
		i++
		for i < n && s[i] == ' ' {
			i++
		}
		var val strings.Builder
		if i < n && s[i] == '\'' {
			i++
			closed := false
			for i < n {
				c := s[i]
				if c == '\\' && i+1 < n {
					val.WriteByte(s[i+1])
					i += 2
					continue
				}
				if c == '\'' {
					i++
					closed = true
					break
				}
				val.WriteByte(c)
				i++
			}
			if !closed {
				return nil, fmt.Errorf("the connection string: the value of %q is not closed", key)
			}
		} else {
			for i < n && s[i] != ' ' {
				if s[i] == '\\' && i+1 < n {
					i++
				}
				val.WriteByte(s[i])
				i++
			}
		}
		kv[key] = val.String()
	}
}

var versionRe = regexp.MustCompile(`(\d+)(?:\.(\d+))?`)

// pgDumpMajor is the major version of the pg_dump bin ("pg_dump (PostgreSQL)
// 17.11 …" is 17), and that line.
func pgDumpMajor(ctx context.Context, bin string) (major int, line string, err error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "--version").Output()
	if err != nil {
		return 0, "", err
	}
	line = strings.TrimSpace(string(out))
	if m := versionRe.FindStringSubmatch(line); m != nil {
		major, _ = strconv.Atoi(m[1])
	}
	if major == 0 {
		err = fmt.Errorf("not a version: %q", line)
	}
	return
}

// serverMajor is the major version of the database server (170004 is 17).
func (c *Controller) serverMajor() (int, error) {
	var num string
	if err := c.getDb().Raw("SHOW server_version_num").Scan(&num).Error; err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(num))
	if err != nil {
		return 0, err
	}
	return n / 10000, nil
}

// checkPgDump says whether pg_dump may back the database up: there, and not
// older than the server (an older one refuses to).
func (c *Controller) checkPgDump(ctx context.Context) (version string, err error) {
	if _, err = c.dsn(); err != nil {
		return
	}
	major, line, err := pgDumpMajor(ctx, c.pgDump)
	if err != nil {
		return "", &MethodError{Reason: ReasonPgDumpMissing, Detail: c.pgDump + ": " + err.Error()}
	}
	server, err := c.serverMajor()
	if err != nil {
		return line, err
	}
	if major < server {
		return line, &MethodError{Reason: ReasonPgDumpOld, Detail: fmt.Sprintf("pg_dump %d < server %d", major, server)}
	}
	return line, nil
}

// dsn is the connection string the application is connected with.
func (c *Controller) dsn() (string, error) {
	if d, ok := c.db.Dialector.(*postgres.Dialector); ok && d.Config != nil && d.Config.DSN != "" {
		return d.Config.DSN, nil
	}
	return "", &MethodError{Reason: ReasonNoDSN}
}

// createPgDump backs the database up with pg_dump, run by the application
// with the connection it uses, into the archive the backups are (Create).
func (c *Controller) createPgDump(ctx context.Context, auto bool, message string) (bkp db_tools.Backuper, err error) {
	var (
		dsn    string
		env    []string
		dbName string
	)
	if dsn, err = c.dsn(); err != nil {
		return
	}
	if env, err = ConnEnv(dsn); err != nil {
		return
	}
	if dbName, err = c.CurrentName(ctx); err != nil {
		return
	}

	ts := time.Now().UTC().Truncate(time.Second)
	tmpDir := filepath.Join(c.localDir, ".tmp")
	if err = os.MkdirAll(tmpDir, 0o770); err != nil {
		return
	}
	tmp, err := os.CreateTemp(tmpDir, "pg_dump-*.pg")
	if err != nil {
		return
	}
	tmpName := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpName)

	cmd := exec.CommandContext(ctx, c.pgDump, "-xO", "-Fc", "-f", tmpName)
	// the connection by the PG* variables alone: the ones of the process out
	cmd.Env = append(withoutPG(os.Environ()), env...)
	var stderr bytes.Buffer
	cmd.Stderr = &limitWriter{w: &stderr, n: 8 << 10}
	if err = cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("pg_dump: %s", msg)
	}

	pth := filepath.Join(c.GetAutoLocalDir(auto), dbName, ts.Format("2006/01/02"), ts.Format(FileNameTimeFormat)+".tar")
	if err = writeArchive(pth, ts, message, tmpName); err != nil {
		return
	}
	return c.Get(db_tools.BackupID{Auto: auto, DbName: dbName, CreatedAt: ts})
}

// withoutPG is env with no PG* variable.
func withoutPG(env []string) (out []string) {
	for _, e := range env {
		if !strings.HasPrefix(e, "PG") {
			out = append(out, e)
		}
	}
	return
}

// writeArchive writes the archive of a backup at pth: its message
// (message.txt) and the copy of the database (backup.pg, the file dump).
// Another archive there already is an error, not overwritten.
func writeArchive(pth string, ts time.Time, message, dump string) (err error) {
	if err = os.MkdirAll(filepath.Dir(pth), 0o770); err != nil {
		return
	}
	in, err := os.Open(dump)
	if err != nil {
		return
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return
	}
	f, err := os.OpenFile(pth, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return
	}
	defer func() {
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			os.Remove(pth)
		}
	}()
	tw := tar.NewWriter(f)
	msg := []byte(message)
	if err = tw.WriteHeader(&tar.Header{Name: "message.txt", Mode: 0o600, Size: int64(len(msg)), ModTime: ts}); err != nil {
		return
	}
	if _, err = tw.Write(msg); err != nil {
		return
	}
	if err = tw.WriteHeader(&tar.Header{Name: "backup.pg", Mode: 0o600, Size: info.Size(), ModTime: ts}); err != nil {
		return
	}
	if _, err = io.Copy(tw, in); err != nil {
		return
	}
	return tw.Close()
}

// limitWriter keeps the first n bytes written (pg_dump's errors).
type limitWriter struct {
	w io.Writer
	n int
}

func (l *limitWriter) Write(p []byte) (int, error) {
	if l.n > 0 {
		k := min(len(p), l.n)
		l.w.Write(p[:k])
		l.n -= k
	}
	return len(p), nil
}
