# Como funciona

A cópia é feita **pelo próprio servidor do banco de dados**, e enviada à
aplicação:

1. A aplicação pede um backup ao banco: chama a função `db_backup__create`,
   que ela instala no banco ao iniciar.
2. A função, escrita em PL/Python (`plpython3u`), roda o `pg_dump` no servidor
   do banco — a cópia do banco, no formato custom do PostgreSQL, sem donos nem
   privilégios (`pg_dump -xO -Fc`).
3. Envia o arquivo à aplicação por HTTP: a aplicação escuta numa porta própria
   (escolhida ao iniciar), no endereço em que o banco a vê.
4. A aplicação guarda o arquivo, e a tarefa diz o que foi feito.

## Os arquivos

Os backups ficam no diretório de dados da aplicação:

```
data/db-backup/
  auto/<banco>/<ano>/<mês>/<dia>/<AAAAMMDDTHHMMSS>.tar
  manual/<banco>/<ano>/<mês>/<dia>/<AAAAMMDDTHHMMSS>.tar
```

O nome é o momento do backup, em UTC. Cada arquivo é um `tar` com:

- `message.txt` — a nota do backup ("auto backup" nos automáticos);
- `backup.pg` — a cópia do banco (formato custom do `pg_dump`), o que o
  `pg_restore` lê.

O que guarda o diretório de dados guarda os backups: num servidor, o diretório
precisa estar num volume que sobreviva a uma nova versão da aplicação.
