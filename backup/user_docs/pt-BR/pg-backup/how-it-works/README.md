# Como funciona

Um backup é feito por um de dois **métodos**, escolhido na página do banco de
dados (**Método de backup**, ao lado do armazenamento). Os dois fazem o mesmo
arquivo.

## PL/Python, pelo servidor do banco

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

## pg_dump, pela aplicação

A cópia é feita **pela aplicação**, com a conexão que ela já usa — nada
instalado no banco, nenhuma porta entre os dois:

1. A aplicação roda o `pg_dump -xO -Fc` (o do `DB_PG_DUMP`, senão o `pg_dump`
   do PATH dela), conectado como ela está conectada: o host, a porta, o
   usuário, a senha e o banco da conexão dela vão ao `pg_dump` pelo ambiente
   (`PGHOST`, `PGPASSWORD`, …) — a senha nunca numa linha de comando.
2. Ela guarda o arquivo, e a tarefa diz o que foi feito. Parar a tarefa para o
   `pg_dump`.

Ao iniciar, a aplicação confere quais métodos podem ser usados: o PL/Python
quando a função dele pôde ser criada, o pg_dump quando ele existe e **não é
mais velho que o servidor do banco**. A página diz por que um não pode. Sem
nenhum escolhido, os backups usam o primeiro que pode ser usado, o PL/Python
primeiro.

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
