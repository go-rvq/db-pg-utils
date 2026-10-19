# Quando um backup falha

Um backup automático que falhou é uma tarefa **Com erro** (Tarefas): o detalhe
dela tem o registro e o erro. Um **Criar backup** que falhou diz o erro na
hora.

| O erro diz | O que aconteceu | O que fazer |
|---|---|---|
| `language "plpython3u" does not exist` | O banco não tem PL/Python. | Instale-o no servidor e crie a extensão (veja Requisitos), ou mantenha os backups desligados. |
| `must be superuser to create … function` | A função de uma linguagem não confiável é criada por um superusuário. | Crie-a uma vez como superusuário, ou dê esse direito ao usuário da aplicação. |
| `Connection refused`, um timeout | O servidor do banco não alcança a aplicação na porta dela. | Coloque os dois na mesma rede; libere a porta entre eles. |
| Um erro do `pg_dump` | A cópia falhou no servidor (uma permissão, o disco). | A mensagem diz qual; olhe o servidor. |
| `No space left on device` | Sem espaço em `/tmp` do servidor, ou no diretório de dados da aplicação. | Libere espaço; diminua a persistência. |
| `pg_dump: … server version mismatch` | O `pg_dump` da aplicação é mais velho que o servidor do banco. | Instale o `pg_dump` da versão do servidor (ou mais nova), ou aponte o `DB_PG_DUMP` para ele. |
| `pg_dump: … connection … failed` | O método pg_dump não conseguiu se conectar como a aplicação se conecta. | A mensagem diz o motivo (o host, a senha); a própria aplicação se conecta com a mesma. |
| `backup method not available` | O método escolhido não pode ser usado agora. | A página do banco de dados diz o motivo; escolha o outro, ou corrija o que ela diz. |
| `file does not exist` | Versões anteriores à correção de outubro de 2026 não achavam o arquivo que tinham acabado de fazer — o backup era feito, a tarefa dizia que falhou, e os automáticos ficavam como manuais. | Atualize a aplicação. Os automáticos guardados como manuais não são removidos pela persistência: remova-os à mão. |
