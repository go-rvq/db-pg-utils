package user_docs

import (
	"github.com/go-rvq/rvq/admin/userdocs"
	"github.com/go-rvq/rvq/x/i18n"
)

// MessagesKey is the module of the words of the documentation of the package.
const MessagesKey i18n.ModuleKey = "db-pg-utils/backup/user_docs"

// Messages are the words of the documentation of the package.
type Messages struct {
	userdocs.PackageMessages
}

var Messages_en_US = &Messages{
	PackageMessages: userdocs.PackageMessagesEn("Backups of the database",
		"How the backups of the PostgreSQL database are made, what they need, how one is restored."),
}

var Messages_pt_BR = &Messages{
	PackageMessages: userdocs.PackageMessagesPtBR("Backups do banco de dados",
		"Como os backups do banco PostgreSQL são feitos, do que precisam, como restaurar um."),
}
