package co.test;

import com.app.executor.CdcExecutor;

public class TestSqlCdc {
    public static void main(String[] args) throws Exception {
        String ds = "with (\n" +
                "    'connector' = 'oracle-cdc',\n" +
                "    'hostname' = '10.1.51.25',\n" +
                "    'port' = '1521',\n" +
                "    'username' = 'cdr',\n" +
                "    'password' = 'cdr',\n" +
                "    'database-name' = 'ORCL',\n" +
                "    'schema-name' = 'TEST'," +
                "    'debezium.decimal.handling.mode'='STRING',\n" +
                "    'sink.db.prefix'='ods',\n" +
                "    'chain.end.name'='CHAIN_END',\n" +
                "    'cus.table.prefix'='ODS_',\n" +
                "    'sink.prop.fenodes'='10.1.51.26:8030',\n" +
                "    'sink.prop.username'='root',\n" +
                "    'sink.prop.password'='dw123456',\n" +
                "    'sink.prop.cus.sink.db'='ods',\n" +
                "    'cus.time.precision' = '1000'\n" +
                "    'cus.time.zone' = '0'\n" +
                "    'table-name' = 'TEST.CDC_TEST'\n" +
                ")";
        System.out.println(ds);
/*        CdcExecutor cdcExecutor = new CdcExecutor();

        cdcExecutor.executeSql("with (\n" +
                "    'connector' = 'oracle-cdc',\n" +
                "    'hostname' = '10.1.51.25',\n" +
                "    'port' = '1521',\n" +
                "    'username' = 'cdr',\n" +
                "    'password' = 'cdr',\n" +
                "    'database-name' = 'ORCL',\n" +
                "    'schema-name' = 'TEST'," +
                "    'debezium.decimal.handling.mode'='STRING',\n" +
                "    'sink.db.prefix'='ods',\n" +
                "    'chain.end.name'='CHAIN_END',\n" +
                "    'cus.table.prefix'='ODS_',\n" +
                "    'sink.prop.fenodes'='10.1.51.26:8030',\n" +
                "    'sink.prop.username'='root',\n" +
                "    'sink.prop.password'='dw123456',\n" +
                "    'sink.prop.cus.sink.db'='ods',\n" +
                "    'cus.time.precision' = '1000'\n" +
                "    'cus.time.zone' = '0'\n" +
                "    'table-name' = 'TEST.CDC_TEST'\n" +
                ")", "4184");*/
    }
}
