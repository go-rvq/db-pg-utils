package backup

import (
	"context"
	"strconv"
	"strings"

	"github.com/go-rvq/rvq/x/i18n"
	"golang.org/x/text/language"
)

const MessagesKey i18n.ModuleKey = "rqv/db-pg-utils"

func GetMessages(ctx context.Context) *Messages {
	return i18n.MustGetModuleMessages(ctx, MessagesKey, Messages_en_US).(*Messages)
}

type Messages struct {
	BackupDetailTemplate string
}

var (
	Messages_en_US = &Messages{
		BackupDetailTemplate: "{db} at {createdAt} with message {message}",
	}

	Messages_pt_BR = &Messages{
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
	b.RegisterForModules(language.English, MessagesKey, Messages_pt_BR).
		RegisterForModules(language.BrazilianPortuguese, MessagesKey, Messages_pt_BR)
}
