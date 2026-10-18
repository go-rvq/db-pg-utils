# Backups do banco de dados

Os backups do banco de dados PostgreSQL: uma cópia de todos os dados, feita
uma vez por dia (o **backup automático**) ou quando alguém pede (**Criar
backup**, na página do banco de dados).

Cada backup é um arquivo guardado pela aplicação, listado na página do banco
de dados, onde pode ser baixado ou removido.

- [Como funciona](how-it-works/README.md) — quem faz a cópia, para onde ela
  vai, o que há no arquivo.
- [Requisitos](requirements/README.md) — do que o servidor do banco e a
  aplicação precisam.
- [Restaurar um backup](restore/README.md) — do arquivo baixado a um banco.
- [Quando um backup falha](troubleshooting/README.md) — os erros e o que fazer.

## Automático e manual

- O **backup automático** roda uma vez por dia, como uma tarefa (Tarefas).
  Depois dele, os backups automáticos mais antigos do que a **persistência**
  permite são removidos: tantos dias, semanas, meses, anos.
- Um **backup manual** (Criar backup) tem uma nota, escrita por quem o fez.
  A persistência nunca o remove: só à mão.
