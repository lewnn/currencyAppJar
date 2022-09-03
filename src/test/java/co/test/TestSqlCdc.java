package co.test;

import com.app.executor.CdcExecutor;

public class TestSqlCdc {
    public static void main(String[] args) throws Exception {
        CdcExecutor cdcExecutor = new CdcExecutor();

        cdcExecutor.executeSql("with (\n" +
                "    'connector' = 'oracle-cdc',\n" +
                "    'hostname' = '10.1.51.25',\n" +
                "    'port' = '1521',\n" +
                "    'username' = 'cdr',\n" +
                "    'password' = 'cdr',\n" +
                "    'database-name' = 'ORCL',\n" +
                "    'schema-name' = 'TEST'," +
                "    'debezium.decimal.handling.mode'='STRING',\n" +
                "    'start.date.name'='s_dt',\n" +
                "    'end.date.name'='e_dt',\n" +
                "    'table-name' = 'TEST.CDC_TEST'\n" +
                ")", "4184");
    }
}
