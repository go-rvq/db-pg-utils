# Quando um backup falha

Um backup automático que falhou é uma tarefa **Com erro** (Tarefas): o detalhe
dela tem o registro e o erro. Um **Criar backup** que falhou diz o erro na
hora.

O que a aplicação encontra ao iniciar está na página do banco de dados: um
método que não pode ser usado aparece como **indisponível**, com o motivo — o
PL/Python de um servidor que não consegue gravar no `/tmp` diz, por exemplo,
*O servidor do banco não consegue fazer a cópia (pg_dump: error: could not open
output file "/tmp/…": Permission denied)*.

| O erro diz | O que aconteceu | O que fazer |
|---|---|---|
| `language "plpython3u" does not exist` | O banco não tem PL/Python. | Instale-o no servidor e crie a extensão (veja Requisitos), ou mantenha os backups desligados. |
| `must be superuser to create … function` | A função de uma linguagem não confiável é criada por um superusuário. | Crie-a uma vez como superusuário, ou dê esse direito ao usuário da aplicação. |
| `Connection refused`, um timeout | O servidor do banco não alcança a aplicação na porta dela. | Coloque os dois na mesma rede; libere a porta entre eles. |
| Um erro do `pg_dump` | A cópia falhou no servidor (uma permissão, o disco). | A mensagem diz qual; olhe o servidor. |
| `No space left on device` | Sem espaço em `/tmp` do servidor, ou no diretório de dados da aplicação. | Libere espaço; diminua a persistência. |
| `PL/Python: pg_dump: error: could not open output file "/tmp/…": Permission denied` | O `pg_dump` do servidor do banco não consegue gravar no `/tmp` dele: não gravável pelo usuário de sistema `postgres` — num contêiner, um `/tmp` montado do servidor com `755` do `root`. | Torne esse diretório `1777` (`chmod 1777 <ele>`, no servidor), ou use o método pg_dump, pela aplicação. |
| `error fetching next item from iterator` | Versões anteriores a outubro de 2026 diziam só isso de um erro no PL/Python: o que aconteceu ficava no detalhe dele. | Atualize a aplicação, que o diz; ou veja-o como abaixo. |
| `pg_dump: … server version mismatch` | O `pg_dump` da aplicação é mais velho que o servidor do banco. | Instale o `pg_dump` da versão do servidor (ou mais nova), ou aponte o `DB_PG_DUMP` para ele. |
| `pg_dump: … connection … failed` | O método pg_dump não conseguiu se conectar como a aplicação se conecta. | A mensagem diz o motivo (o host, a senha); a própria aplicação se conecta com a mesma. |
| `backup method not available` | O método escolhido não pode ser usado agora. | A página do banco de dados diz o motivo; escolha o outro, ou corrija o que ela diz. |
| `file does not exist` | Versões anteriores à correção de outubro de 2026 não achavam o arquivo que tinham acabado de fazer — o backup era feito, a tarefa dizia que falhou, e os automáticos ficavam como manuais. | Atualize a aplicação. Os automáticos guardados como manuais não são removidos pela persistência: remova-os à mão. |

## Ver o erro do PL/Python

A função pode ser chamada à mão, e o `psql` mostra o erro inteiro (o `DETAIL`
dele) — no servidor do banco, como o usuário da aplicação, com o endereço e a
porta em que a aplicação escuta os backups (no log dela:
`db-pg-utils:storage_server … Listening on port`):

```sh
psql -U <usuário> <banco> -c "SELECT * FROM db_backup__create('teste', '<endereço da aplicação>:<porta>', 'auto=false')"
```

Quando funciona, faz um backup manual (com a nota "teste"), listado na
página do banco de dados.
