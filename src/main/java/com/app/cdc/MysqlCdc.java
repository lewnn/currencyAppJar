package com.app.cdc;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;

public class MysqlCdc extends BaseCdc {
    public static String type = "mysql-cdc";

    @Override
    public String getType() {
        return MysqlCdc.type;
    }

    @Override
    public boolean checkSourceAndDb(HashMap source) {
        return source.containsKey("db") && source.containsKey("table");
    }

    @Override
    public String getOutputTagName(HashMap source) {
        return source.get("db") + "." + source.get("table");
    }

    @Override
    public String getTablesColumnsInfo(String db, String tableName) {
        return "SELECT    COLUMN_NAME AS 'col',DATA_TYPE AS 'type', " +
                "    CHARACTER_MAXIMUM_LENGTH AS 'varlen',NUMERIC_PRECISION AS 'numlen', " +
                "    NUMERIC_SCALE AS 'scale' " +
                "FROM  " +
                "    information_schema.`COLUMNS` " +
                "WHERE " +
                "    TABLE_SCHEMA =  '" + db + "'" +
                "    and TABLE_NAME= '" + tableName + "'" +
                "    ORDER by ORDINAL_POSITION";
    }


    private MysqlCdc(String sql) {
        parseConfig(sql);
    }

    public static MysqlCdc getInstance(String sql) {
        return new MysqlCdc(sql);
    }

    @Override
    public DataStream<String> addSource(StreamExecutionEnvironment environment) {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(this.getHostname())
                .port(this.getPort())
                .databaseList(this.getDataBaseName().split(","))
                .tableList(this.getTables().split(","))
                .username(this.getUserName())
                .password(this.getPassword())
                .serverTimeZone("UTC")  //时区
                .debeziumProperties(this.getDebeziumProperties())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
       return environment.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-source");
    }
}
