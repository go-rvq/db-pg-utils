package backup

import (
	"context"
	"fmt"
	"strings"
	"sync"

	db_tools "github.com/go-rvq/rvq/x/packages/db-tools"
)

// Method is how a backup is made.
type Method string

const (
	// MethodPLPython: by the database server — a function in PL/Python
	// (plpython3u) runs pg_dump there and sends the file to the application
	// by HTTP.
	MethodPLPython Method = "plpython"
	// MethodPgDump: by the application — it runs pg_dump (DB_PG_DUMP) with the
	// connection it uses; nothing installed in the database, no server.
	MethodPgDump Method = "pg_dump"
)

// Methods are the methods, in the order they are offered (and the default
// is the first available).
var Methods = []Method{MethodPLPython, MethodPgDump}

// Reason is why a method is not available.
type Reason string

const (
	ReasonNoPLPython    Reason = "no_plpython"
	ReasonPLPythonFails Reason = "plpython_fails"
	ReasonPgDumpMissing Reason = "pg_dump_missing"
	ReasonPgDumpOld     Reason = "pg_dump_old"
	ReasonNoDSN         Reason = "no_dsn"
	ReasonUnknown       Reason = "unknown"
)

// MethodError is a method not available: why (Reason, said in the user's
// language by Messages.ReasonText) and what was found (Detail).
type MethodError struct {
	Reason Reason
	Detail string
}

// Error is the error as the log says it: why, in English, and what was
// found.
func (e *MethodError) Error() string {
	t := "backup method not available: " + Messages_en_US.ReasonText(e.Reason)
	if e.Detail != "" {
		t += " (" + e.Detail + ")"
	}
	return t
}

// Text is the error in ctx's language.
func (e *MethodError) Text(ctx context.Context) string {
	t := GetMessages(ctx).ReasonText(e.Reason)
	if e.Detail != "" {
		t += " (" + e.Detail + ")"
	}
	return t
}

// MethodInfo is a method as it is offered: whether it is available, why
// not, what makes it (the version of pg_dump).
type MethodInfo struct {
	Method    Method
	Label     string
	Available bool
	Reason    string
	Version   string
}

type methods struct {
	mu        sync.RWMutex
	chosen    Method
	plpyErr   error
	pgDumpErr error
	pgDumpVer string
}

// MethodsInfo are the methods, in ctx's language.
func (c *Controller) MethodsInfo(ctx context.Context) (out []MethodInfo) {
	c.m.mu.RLock()
	defer c.m.mu.RUnlock()
	m := GetMessages(ctx)
	for _, meth := range Methods {
		info := MethodInfo{Method: meth, Label: m.MethodLabel(meth), Available: true}
		var err error
		switch meth {
		case MethodPLPython:
			err = c.m.plpyErr
		case MethodPgDump:
			err, info.Version = c.m.pgDumpErr, c.m.pgDumpVer
		}
		if err != nil {
			info.Available, info.Reason = false, errorText(ctx, err)
		}
		out = append(out, info)
	}
	return
}

func errorText(ctx context.Context, err error) string {
	if me, ok := err.(*MethodError); ok {
		return me.Text(ctx)
	}
	return err.Error()
}

// Method is the method backups are made with: the one chosen (SetMethod),
// else the first available.
func (c *Controller) Method() Method {
	c.m.mu.RLock()
	defer c.m.mu.RUnlock()
	if c.m.chosen != "" {
		return c.m.chosen
	}
	for _, meth := range Methods {
		if c.unavailable(meth) == nil {
			return meth
		}
	}
	return MethodPLPython
}

// SetMethod chooses the method backups are made with ("": the default). A
// method not available may be chosen — its backups fail saying why.
func (c *Controller) SetMethod(name string) error {
	meth := Method(strings.TrimSpace(name))
	if meth != "" && meth != MethodPLPython && meth != MethodPgDump {
		return fmt.Errorf("backup: no method %q", name)
	}
	c.m.mu.Lock()
	c.m.chosen = meth
	c.m.mu.Unlock()
	return nil
}

// unavailable is why meth is not available (nil: it is); mu held.
func (c *Controller) unavailable(meth Method) error {
	switch meth {
	case MethodPLPython:
		return c.m.plpyErr
	case MethodPgDump:
		return c.m.pgDumpErr
	}
	return &MethodError{Reason: ReasonUnknown, Detail: string(meth)}
}

// Create makes a backup now, with the method chosen.
func (c *Controller) Create(auto bool, message string) (db_tools.Backuper, error) {
	return c.CreateContext(context.Background(), auto, message)
}

// CreateContext is Create: ctx done stops pg_dump (the method pg_dump).
func (c *Controller) CreateContext(ctx context.Context, auto bool, message string) (db_tools.Backuper, error) {
	meth := c.Method()
	c.m.mu.RLock()
	err := c.unavailable(meth)
	c.m.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if meth == MethodPgDump {
		return c.createPgDump(ctx, auto, message)
	}
	return c.createPLPython(auto, message)
}
