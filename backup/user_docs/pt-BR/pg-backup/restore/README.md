# Restaurar um backup

A restauração é feita fora do admin, por quem administra o servidor do banco,
com as ferramentas do PostgreSQL (`pg_restore`, da versão do servidor ou mais
nova).

1. **Baixe** o backup, na página do banco de dados: um arquivo `.tar`.
2. **Tire a cópia** de dentro dele:

   ```sh
   tar -xf 20261018T030000.tar backup.pg
   ```

3. **Confira** antes de restaurar — a lista do que ele tem:

   ```sh
   pg_restore --list backup.pg | head
   ```

4. **Restaure num banco novo**, e confira lá:

   ```sh
   createdb restaurado
   pg_restore --no-owner --no-privileges -d restaurado backup.pg
   ```

## Sobre o banco em uso

Restaurar sobre o banco que a aplicação usa **substitui os dados dele**:

- faça um backup dele antes (Criar backup), e baixe-o;
- pare a aplicação durante a restauração;
- restaure com `--clean --if-exists`, que apaga o que existe antes:

  ```sh
  pg_restore --clean --if-exists --no-owner --no-privileges -d <banco> backup.pg
  ```

- inicie a aplicação de novo, e confira o site e o admin.
