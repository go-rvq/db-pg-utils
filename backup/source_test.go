package backup

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-rvq/rvq/x/i18n"
	rvqbackup "github.com/go-rvq/rvq/x/packages/backup"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// The database in a backup state: its dump, of pg_dump's custom format,
// kept in the state; its meta.
func TestBackupSource(t *testing.T) {
	dsn := os.Getenv("DB_PG_UTILS_TEST_DSN")
	if dsn == "" {
		t.Skip("DB_PG_UTILS_TEST_DSN is not set")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Skipf("no database: %v", err)
	}
	c := New(db, i18n.New())
	if _, err := c.checkPgDump(context.Background()); err != nil {
		t.Skipf("pg_dump not available: %v", err)
	}
	root := t.TempDir()
	b, err := rvqbackup.New(rvqbackup.Options{Root: root, Dir: filepath.Join(t.TempDir(), "states")})
	if err != nil {
		t.Fatal(err)
	}
	b.Source("db", c.BackupSource(""))
	st := &rvqbackup.State{ID: "s"}
	if err := b.BuildState(context.Background(), st); err != nil {
		t.Fatal(err)
	}
	m, err := b.Manifest("s")
	if err != nil {
		t.Fatal(err)
	}
	name, _ := c.CurrentName(context.Background())
	p := "data/db/" + name + ".pg"
	if m.Header.Meta["db.dump"] != p || m.Header.Meta["db.format"] != "pg_dump-custom" || m.Header.Meta["db.name"] != name {
		t.Errorf("meta %v", m.Header.Meta)
	}
	e, ok := m.Find(p)
	if !ok || e.Size == 0 || e.Mode != 0o600 {
		t.Fatalf("dump %+v", e)
	}
	f, err := b.OpenFile("s", e)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	head := make([]byte, 5)
	io.ReadFull(f, head)
	if string(head) != "PGDMP" {
		t.Errorf("not a pg_dump custom archive: %q", head)
	}
	if _, err := os.Stat(filepath.Join(root, p)); err == nil {
		t.Error("the dump written in the application's directory")
	}
	if pr := os.Getenv("DB_PG_RESTORE"); pr != "" {
		if o, err := exec.Command(pr, "--list", f.Name()).CombinedOutput(); err != nil || !strings.Contains(string(o), "TABLE") {
			t.Errorf("pg_restore --list: %v\n%s", err, o)
		}
	}
}
