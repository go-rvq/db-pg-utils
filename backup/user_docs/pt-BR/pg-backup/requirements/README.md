# Requisitos

Basta um método que possa ser usado — a página do banco de dados diz quais
podem, e por que não.

## PL/Python, pelo servidor do banco

- **PL/Python** (`plpython3u`) no servidor do banco: o pacote dele instalado
  (no Debian, `postgresql-plpython3-<versão>`), e a linguagem criada no banco
  (`CREATE EXTENSION plpython3u`).
- O usuário da aplicação cria a função ao iniciar: uma função de linguagem não
  confiável (`plpython3u`) só é criada por um **superusuário**.
  Se o usuário da aplicação é um:
  `SELECT rolsuper FROM pg_roles WHERE rolname = current_user;` (`t`: é).
- O `pg_dump` no servidor — vem com o PostgreSQL, da versão dele.
- **O `/tmp` do servidor gravável pelo usuário com que o PostgreSQL roda** (o
  usuário de sistema `postgres`), com espaço para uma cópia enquanto ela é
  enviada: é lá que o `pg_dump` a grava. Um `/tmp` é `1777` (todos gravam, cada
  um remove o seu); num contêiner cujo `/tmp` é um diretório do servidor
  montado nele, esse diretório precisa ser `1777` também — um `755` do `root`
  faz todo backup falhar com `Permission denied`.
- O servidor do banco **alcança a aplicação**: no endereço em que a vê, na
  porta em que a aplicação escuta os backups (uma porta livre, escolhida ao
  iniciar — no log: `db-pg-utils:storage_server … Listening on port`). Em
  contêineres, os dois na mesma rede; nenhum firewall entre eles.

## pg_dump, pela aplicação

- **O `pg_dump` onde a aplicação roda**, da versão principal do servidor do
  banco **ou mais nova** — um mais velho se recusa a fazer o backup. No
  Debian, o pacote `postgresql-client-<versão>` (para uma versão mais nova que
  a da distribuição, do repositório do PostgreSQL, apt.postgresql.org).
- Onde ele está: `DB_PG_DUMP` (no `env:` da configuração), um caminho ou um
  nome no PATH — `pg_dump` quando não definido.
- O usuário da aplicação pode ler o banco inteiro (o que ele copia).

## Na aplicação, para os dois

- **Espaço no diretório de dados** (`data/db-backup`) para os backups que a
  persistência guarda.
- Um diretório de dados que sobreviva a uma nova versão da aplicação (um
  volume).
