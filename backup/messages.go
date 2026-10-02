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
}

var (
	Messages_en_US = &Messages{
		ModuleDescription:    "The PostgreSQL database backups: making, listing and restoring them.",
		BackupDetailTemplate: "{db} at {createdAt} with message {message}",
	}

	Messages_pt_BR = &Messages{
		ModuleDescription:    "As cópias de segurança do banco PostgreSQL: gerar, listar e restaurar.",
		BackupDetailTemplate: "{db} em {createdAt} com a mensagem {message}",
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

func ConfigureMessages(b *i18n.Builder) {
	b.RegisterForModules(language.English, MessagesKey, Messages_en_US).
		RegisterForModules(language.BrazilianPortuguese, MessagesKey, Messages_pt_BR)
}
