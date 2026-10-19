package backup

import (
	"context"
	"strconv"
	"strings"

	"github.com/go-rvq/rvq/x/i18n"
	"golang.org/x/text/language"
)

const MessagesKey i18n.ModuleKey = "rvq/db-pg-utils"

func GetMessages(ctx context.Context) *Messages {
	return i18n.MustGetModuleMessages(ctx, MessagesKey, Messages_en_US).(*Messages)
}

type Messages struct {
	ModuleDescription    string `i18n:"hint='Says what the module is, for whoever translates its texts.'"`
	BackupDetailTemplate string `i18n:"label='Backup detail', hint='A database backup, in a line.', fields=(;'{db}'='the database', '{createdAt}'='when it was made', '{message}'='its message, quoted')"`
	MethodPLPython       string `i18n:"label='Method: PL/Python', hint='The backup method made by the database server (a PL/Python function runs pg_dump there).'"`
	MethodPgDump         string `i18n:"label='Method: pg_dump', hint='The backup method made by the application (it runs pg_dump with its connection).'"`
	ReasonNoPLPython     string `i18n:"hint='Why the method PL/Python is not available: the database has no plpython3u, or the function could not be created.'"`
	ReasonPLPythonFails  string `i18n:"hint='Why the method PL/Python is not available: the database server cannot make the copy (its pg_dump, its /tmp); the error follows.'"`
	ReasonPgDumpMissing  string `i18n:"hint='Why the method pg_dump is not available: no pg_dump where the application looks for it.'"`
	ReasonPgDumpOld      string `i18n:"hint='Why the method pg_dump is not available: it is older than the database server.'"`
	ReasonNoDSN          string `i18n:"hint='Why the method pg_dump is not available: the connection string of the application is not known.'"`
	ReasonUnknown        string `i18n:"hint='A backup method that does not exist.'"`
}

var (
	Messages_en_US = &Messages{
		ModuleDescription:    "The PostgreSQL database backups: making, listing and restoring them.",
		BackupDetailTemplate: "{db} at {createdAt} with message {message}",
		MethodPLPython:       "PL/Python, by the database server",
		MethodPgDump:         "pg_dump, by the application",
		ReasonNoPLPython:     "The database has no PL/Python (plpython3u), or its function could not be created.",
		ReasonPLPythonFails:  "The database server cannot make the copy.",
		ReasonPgDumpMissing:  "There is no pg_dump where the application looks for it (DB_PG_DUMP).",
		ReasonPgDumpOld:      "The pg_dump is older than the database server: it refuses to back it up.",
		ReasonNoDSN:          "The connection string of the application is not known.",
		ReasonUnknown:        "No such method.",
	}

	Messages_pt_BR = &Messages{
		ModuleDescription:    "As cópias de segurança do banco PostgreSQL: gerar, listar e restaurar.",
		BackupDetailTemplate: "{db} em {createdAt} com a mensagem {message}",
		MethodPLPython:       "PL/Python, pelo servidor do banco",
		MethodPgDump:         "pg_dump, pela aplicação",
		ReasonNoPLPython:     "O banco não tem PL/Python (plpython3u), ou a função dele não pôde ser criada.",
		ReasonPLPythonFails:  "O servidor do banco não consegue fazer a cópia.",
		ReasonPgDumpMissing:  "Não há pg_dump onde a aplicação o procura (DB_PG_DUMP).",
		ReasonPgDumpOld:      "O pg_dump é mais velho que o servidor do banco: ele se recusa a fazer o backup.",
		ReasonNoDSN:          "A conexão da aplicação com o banco não é conhecida.",
		ReasonUnknown:        "Esse método não existe.",
	}
)

func (m *Messages) BackupDetail(dbName, createdAt, message string) string {
	return strings.NewReplacer(
		"{db}", dbName,
		"{createdAt}", createdAt,
		"{message}", strconv.Quote(message),
	).
		Replace(m.BackupDetailTemplate)
}

// MethodLabel is the name of meth.
func (m *Messages) MethodLabel(meth Method) string {
	switch meth {
	case MethodPLPython:
		return m.MethodPLPython
	case MethodPgDump:
		return m.MethodPgDump
	}
	return string(meth)
}

// ReasonText says why a method is not available.
func (m *Messages) ReasonText(r Reason) string {
	switch r {
	case ReasonNoPLPython:
		return m.ReasonNoPLPython
	case ReasonPLPythonFails:
		return m.ReasonPLPythonFails
	case ReasonPgDumpMissing:
		return m.ReasonPgDumpMissing
	case ReasonPgDumpOld:
		return m.ReasonPgDumpOld
	case ReasonNoDSN:
		return m.ReasonNoDSN
	}
	return m.ReasonUnknown
}

func ConfigureMessages(b *i18n.Builder) {
	b.RegisterForModules(language.English, MessagesKey, Messages_en_US).
		RegisterForModules(language.BrazilianPortuguese, MessagesKey, Messages_pt_BR)
}
