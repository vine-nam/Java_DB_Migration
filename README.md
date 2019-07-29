# Java_DBtoDB_Migration

## Running
```shell
java -Xms20480m -Xmx204800m -jar migration.jar MigrationFile.sql DBConnectionInfoFile.txt 2>&1 | tee -a logs/Migration.log
```

### Argument
1. MigrationFile.sql(required)
```sql
/*comment can be used.*/
--source_DB,source_Table,target_DB,target_Table
LOCAL,A.A_TABLE,LOCAL,B.B_TABLE--test sample
```

2. DBConnectionInfoFile.txt(required)
```sql
--db_name/user/password/ip:port:sid
LOCAL/scott/tiger/localhost:1521:orcl
```

3. Batch size(optional, default: 50000)

### Log
```
[11:07:34] A.A_TABLE -> B.B_TABLE
[11:07:34] ★★★
[11:08:12] 총 갯수:     101483
```
