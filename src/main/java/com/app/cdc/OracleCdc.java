package com.app.cdc;

import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;

public class OracleCdc extends BaseCdc {
    public static String type = "oracle-cdc";

    @Override
    public String getSchemaName() {
        return cdcProper.getProperty("schema-name");
    }

    @Override
    public String getType() {
        return OracleCdc.type;
    }

    @Override
    public boolean checkSourceAndDb(HashMap source) {
        return source.containsKey("schema") && source.containsKey("table");
    }

    @Override
    public String getOutputTagName(HashMap source) {
        return source.get("schema") + "." + source.get("table");
    }

    @Override
    public String getTablesColumnsInfo(String db, String tableName) {
        return " SELECT  COLUMN_NAME AS \"col\" ,DATA_TYPE AS \"type\",\n" +
                "        DATA_LENGTH AS \"varlen\", DATA_PRECISION AS \"numlen\",\n" +
                "        DATA_SCALE AS \"SCALE\" FROM ALL_TAB_COLUMNS  WHERE OWNER ='TEST' AND TABLE_NAME ='CDC_TEST'  ORDER  BY COLUMN_ID";
    }

    private OracleCdc(String sql) {
        parseConfig(sql);
    }

    public static OracleCdc getInstance(String sql) {
        return new OracleCdc(sql);
    }

    @Override
    public DataStream<String> addSource(StreamExecutionEnvironment environment) {
        SourceFunction<String> sourceFunction = OracleSource.<String>builder()
                .hostname(this.getHostname())
                .port(this.getPort())
                .database(this.getDataBaseName())
                .schemaList(this.getSchemaName().split(","))
                .tableList(this.getTables())
                .username(this.getUserName())
                .password(this.getPassword())
                .debeziumProperties(this.getDebeziumProperties())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.latest())
                .build();
        return environment.addSource(sourceFunction);
    }

}
