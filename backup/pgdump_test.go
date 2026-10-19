package backup

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-rvq/rvq/x/i18n"
	db_tools "github.com/go-rvq/rvq/x/packages/db-tools"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestConnEnv(t *testing.T) {
	for _, c := range []struct {
		dsn  string
		want []string
	}{
		{`user=app password=s3cret dbname=app sslmode=disable host=127.0.0.1 port=5440 TimeZone=America/Sao_Paulo`,
			[]string{"PGDATABASE=app", "PGHOST=127.0.0.1", "PGPASSWORD=s3cret", "PGPORT=5440", "PGSSLMODE=disable", "PGUSER=app"}},
		{`host = db  password='a b\'c\\d' user=x`,
			[]string{"PGHOST=db", `PGPASSWORD=a b'c\d`, "PGUSER=x"}},
		{`postgres://app:p%40ss@db.local:5433/site?sslmode=require&TimeZone=UTC`,
			[]string{"PGDATABASE=site", "PGHOST=db.local", "PGPASSWORD=p@ss", "PGPORT=5433", "PGSSLMODE=require", "PGUSER=app"}},
	} {
		env, err := ConnEnv(c.dsn)
		if err != nil {
			t.Fatalf("%s: %v", c.dsn, err)
		}
		slices.Sort(env)
		if strings.Join(env, "|") != strings.Join(c.want, "|") {
			t.Errorf("%s:\n got %q\nwant %q", c.dsn, env, c.want)
		}
	}
	for _, bad := range []string{`host`, `password='open`} {
		if _, err := ConnEnv(bad); err == nil {
			t.Errorf("%q: no error", bad)
		}
	}
}

func TestWithoutPG(t *testing.T) {
	got := withoutPG([]string{"PATH=/bin", "PGPASSWORD=x", "HOME=/h", "PGHOST=y"})
	if strings.Join(got, ",") != "PATH=/bin,HOME=/h" {
		t.Errorf("%q", got)
	}
}

func TestWriteArchive(t *testing.T) {
	dir := t.TempDir()
	dump := filepath.Join(dir, "dump.pg")
	if err := os.WriteFile(dump, []byte("PGDMP fake"), 0o600); err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 10, 18, 3, 0, 0, 0, time.UTC)
	c := &Controller{localDir: dir}
	pth := filepath.Join(c.GetAutoLocalDir(true), "app", ts.Format("2006/01/02"), ts.Format(FileNameTimeFormat)+".tar")
	if err := writeArchive(pth, ts, "auto backup", dump); err != nil {
		t.Fatal(err)
	}
	if err := writeArchive(pth, ts, "again", dump); err == nil {
		t.Error("an archive there already was overwritten")
	}
	f, err := c.FileOf(db_tools.BackupID{Auto: true, DbName: "app", CreatedAt: ts})
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.Read()
	if err != nil {
		t.Fatal(err)
	}
	if b.Message != "auto backup" || b.Size != int64(len("PGDMP fake")) || !b.Auto {
		t.Errorf("%+v", b.Backup)
	}
}

// TestCreatePgDump backs up a database with the method pg_dump. It needs a
// database (DB_PG_UTILS_TEST_DSN, e.g. HermonCMS's test one) and a pg_dump
// not older than it (DB_PG_DUMP, else pg_dump): without them, skipped.
func TestCreatePgDump(t *testing.T) {
	dsn := os.Getenv("DB_PG_UTILS_TEST_DSN")
	if dsn == "" {
		t.Skip("DB_PG_UTILS_TEST_DSN is not set")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Skipf("no database: %v", err)
	}
	dir := t.TempDir()
	c := New(db, i18n.New())
	c.localDir = dir
	if v, err := c.checkPgDump(context.Background()); err != nil {
		t.Skipf("pg_dump not available: %v", err)
	} else {
		c.m.pgDumpVer = v
	}
	c.m.plpyErr = &MethodError{Reason: ReasonNoPLPython}
	if c.Method() != MethodPgDump {
		t.Fatalf("the default with no PL/Python: %s", c.Method())
	}

	for _, auto := range []bool{true, false} {
		b, err := c.Create(auto, "test")
		if err != nil {
			t.Fatalf("auto %v: %v", auto, err)
		}
		id := b.GetID()
		if id.Auto != auto || id.DbName == "" || b.GetSize() == 0 {
			t.Errorf("auto %v: %+v size %d", auto, id, b.GetSize())
		}
		sub := "manual"
		if auto {
			sub = "auto"
		}
		f, err := c.FileOf(id)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(f.Path, string(filepath.Separator)+sub+string(filepath.Separator)) {
			t.Errorf("auto %v in %s", auto, f.Path)
		}
		// what pg_restore reads: its custom format (PGDMP); listed by the
		// pg_restore of DB_PG_RESTORE, when given (of pg_dump's version)
		rc, err := b.Open()
		if err != nil {
			t.Fatal(err)
		}
		data := filepath.Join(dir, "x.pg")
		out, _ := os.Create(data)
		_, _ = out.ReadFrom(rc)
		out.Close()
		rc.Close()
		if head, _ := os.ReadFile(data); !strings.HasPrefix(string(head), "PGDMP") {
			t.Errorf("auto %v: not a pg_dump custom archive", auto)
		}
		if pr := os.Getenv("DB_PG_RESTORE"); pr != "" {
			if o, err := exec.Command(pr, "--list", data).CombinedOutput(); err != nil {
				t.Errorf("pg_restore --list: %v\n%s", err, o)
			}
		}
	}
	// no tmp left
	if left, _ := filepath.Glob(filepath.Join(dir, ".tmp", "*")); len(left) > 0 {
		t.Errorf("tmp left: %v", left)
	}
}

func TestChooseMethod(t *testing.T) {
	c := &Controller{}
	c.m.pgDumpErr = &MethodError{Reason: ReasonPgDumpOld, Detail: "pg_dump 16 < server 17"}
	if c.Method() != MethodPLPython {
		t.Errorf("default: %s", c.Method())
	}
	if err := c.SetMethod("pg_dump"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Create(false, "x"); err == nil || !strings.Contains(err.Error(), Messages_en_US.ReasonPgDumpOld) {
		t.Errorf("an unavailable method: %v", err)
	}
	if err := c.SetMethod("nope"); err == nil {
		t.Error("no such method: no error")
	}
	if err := c.SetMethod(""); err != nil || c.Method() != MethodPLPython {
		t.Errorf("back to the default: %v %s", err, c.Method())
	}
}

func TestPlpyError(t *testing.T) {
	err := plpyError(&pgconn.PgError{Message: "error fetching next item from iterator",
		Detail: "plpy.Error: pg_dump: error: could not open output file \"/tmp/x.pg\": Permission denied\n"})
	if err.Error() != `PL/Python: pg_dump: error: could not open output file "/tmp/x.pg": Permission denied` {
		t.Errorf("%q", err)
	}
	other := errors.New("x")
	if plpyError(other) != other {
		t.Error("another error changed")
	}
}
