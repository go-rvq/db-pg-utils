# Requisitos

## No servidor do banco de dados

- **PL/Python** (`plpython3u`): a linguagem da função que faz o backup. O
  pacote dele instalado no servidor (no Debian,
  `postgresql-plpython3-<versão>`), e a linguagem criada no banco
  (`CREATE EXTENSION plpython3u`).
- O usuário da aplicação cria a função ao iniciar: uma função de linguagem não
  confiável (`plpython3u`) só é criada por um **superusuário**.
- O `pg_dump` no servidor — vem com o PostgreSQL, da versão dele.
- **Espaço em `/tmp`** do servidor para uma cópia enquanto ela é enviada.

## Entre os dois

- O servidor do banco **alcança a aplicação**: no endereço em que a vê, na
  porta em que a aplicação escuta os backups (uma porta livre, escolhida ao
  iniciar — no log: `db-pg-utils:storage_server … Listening on port`). Em
  contêineres, os dois na mesma rede; nenhum firewall entre eles.

## Na aplicação

- **Espaço no diretório de dados** (`data/db-backup`) para os backups que a
  persistência guarda.
- Um diretório de dados que sobreviva a uma nova versão da aplicação (um
  volume).

Sem PL/Python no banco, a aplicação não inicia os backups: ela é configurada
com eles desligados (quem administra o servidor decide).
