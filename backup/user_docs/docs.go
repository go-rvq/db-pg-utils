// Package user_docs is the documentation of github.com/go-rvq/db-pg-utils/backup
// for whoever uses the admin (userdocs), in en/ and pt-BR/: how the backups
// of the database are made, what they need, how one is restored, what to do
// when one fails.
//
// Its documents are no part of the admin: the application puts them in the
// tree where it wants (userdocs.Builder.Custom, Nodes) — under the page of
// the database, say:
//
//	docs.Register(user_docs.Source())
//	docs.Custom(userdocs.Custom{Parent: "pages/db-tools", Nodes: []*userdocs.CustomNode{user_docs.Nodes()}})
package user_docs

import (
	"embed"

	"github.com/go-rvq/rvq/admin/userdocs"
	"github.com/go-rvq/rvq/x/i18n"
	"golang.org/x/text/language"
)

// FS is the documents, in the directory of their language.
//
//go:embed en pt-BR
var FS embed.FS

// Package is the package documented.
const Package = "github.com/go-rvq/db-pg-utils/backup"

// Guide is the node of the guide (its ID: its directory).
const Guide = "pg-backup"

// Source is the documentation of the package.
func Source() *userdocs.Source {
	return &userdocs.Source{
		Package:     Package,
		FS:          FS,
		MessagesKey: MessagesKey,
		Messages: map[language.Tag]i18n.Messages{
			language.English:             Messages_en_US,
			language.BrazilianPortuguese: Messages_pt_BR,
		},
	}
}

// Nodes are the nodes of the guide: its page, a page a subject.
func Nodes() *userdocs.CustomNode {
	n := &userdocs.CustomNode{ID: Guide, Icon: "mdi-database-export-outline"}
	for _, s := range []struct{ id, icon string }{
		{"how-it-works", "mdi-cog-outline"},
		{"requirements", "mdi-clipboard-check-outline"},
		{"restore", "mdi-database-arrow-up-outline"},
		{"troubleshooting", "mdi-lifebuoy"},
	} {
		n.Children = append(n.Children, &userdocs.CustomNode{ID: Guide + "/" + s.id, Icon: s.icon})
	}
	return n
}
