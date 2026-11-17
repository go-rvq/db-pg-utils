package backup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"

	rvqbackup "github.com/go-rvq/rvq/x/packages/backup"
)

// DumpTo writes the database to w as pg_dump makes it — its custom format
// (-Fc: what pg_restore reads), no owner, no privileges —, run by the
// application with the connection it uses (by the PG* variables: the
// password never on its command line).
func (c *Controller) DumpTo(ctx context.Context, w io.Writer) error {
	if _, err := c.checkPgDump(ctx); err != nil {
		return err
	}
	dsn, err := c.dsn()
	if err != nil {
		return err
	}
	env, err := ConnEnv(dsn)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, c.pgDump, "-xO", "-Fc")
	cmd.Env = append(withoutPG(os.Environ()), env...)
	cmd.Stdout = w
	var stderr bytes.Buffer
	cmd.Stderr = &limitWriter{w: &stderr, n: 8 << 10}
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("pg_dump: %s", msg)
	}
	return nil
}

// BackupSource is the database in a backup state (rvq x/packages/backup):
// its dump (DumpTo) at p of the state — "": data/db/<database>.pg —, made
// then, kept in the state alone. Meta: "dump" (its path), "format"
// (pg_dump-custom: pg_restore restores it), "name" (the database's).
func (c *Controller) BackupSource(p string) rvqbackup.SourceFunc {
	return func(ctx context.Context, st *rvqbackup.State) error {
		name, err := c.CurrentName(ctx)
		if err != nil {
			return err
		}
		if p == "" {
			p = path.Join("data", "db", name+".pg")
		}
		if err := st.FS.AddGenerated(p, 0o600, func(w io.Writer) error { return c.DumpTo(ctx, w) }); err != nil {
			return err
		}
		for k, v := range map[string]string{"dump": p, "format": "pg_dump-custom", "name": name} {
			if err := st.FS.Meta(k, v); err != nil {
				return err
			}
		}
		return nil
	}
}
