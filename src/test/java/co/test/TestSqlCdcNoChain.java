package co.test;

import com.app.cdc.BaseCdc;
import com.app.executor.CdcExecutor;
import org.apache.flink.util.OutputTag;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TestSqlCdcNoChain {
    public static void main(String[] args) throws Exception {
        String sql = "insert into cdcTable with (\n" +
                "    'connector' = 'oracle-cdc',\n" +
                "    'hostname' = '10.1.51.25',\n" +
                "    'port' = '1521',\n" +
                "    'username' = 'cdr',\n" +
                "    'password' = 'cdr',\n" +
                "    'database-name' = 'ORCL',\n" +
                "    'schema-name' = 'TEST',\n" +
                "\t'table-name' = 'TEST.CDC_TEST',\n" +
                "    'debezium.decimal.handling.mode'='STRING',\n" +
                "    'sink.db.prefix'='ods',\n" +
                "    'chain.end.name'='CHAIN_END',\n" +
                "    'cus.table.prefix'='ODS_',\n" +
                "    'sink.prop.fenodes'='10.1.51.26:8030',\n" +
                "    'sink.prop.username'='root',\n" +
                "    'sink.prop.password'='dw123456',\n" +
                "    'sink.prop.cus.sink.db'='test',\n" +
                "\t'sink.prop.cus.sink.table'='ODS_CDC_TEST_ON',\n" +
                "    'sink.properties.format'='json',\n" +
                "    'sink.properties.read_json_by_line'='true',\n" +
                "    'cus.time.precision' = '1000',\n" +
                "    'cus.time.zone' = '0',\n" +
                "    'cus.time.checkpointing' = '10000',\n" +
                "\t'cus.open.chain' = 'true'\n" +
                ")\n";
        BaseCdc baseCdc = BaseCdc.getInstance(sql, "4313");
        ConcurrentHashMap<String, OutputTag<Map>> outputTagMap = new ConcurrentHashMap<>();
        for (String tag : baseCdc.getTables().split(",")) {
            outputTagMap.put(tag, new OutputTag<Map>(tag) {
            });
        }
        CdcExecutor cdcExecutor = new CdcExecutor(outputTagMap);
        cdcExecutor.executeSql(baseCdc);
    }
}