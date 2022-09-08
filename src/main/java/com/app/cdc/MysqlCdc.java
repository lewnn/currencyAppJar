package com.app.cdc;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class MysqlCdc extends BaseCdc {
    public static String type = "mysql-cdc";

    @Override
    public String getConnect() {
        return null;
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public String getUserName() {
        return null;
    }

    @Override
    public String getPassword() {
        return null;
    }

    @Override
    public String getDataBase() {
        return null;
    }

    @Override
    public String getDataBaseName() {
        return null;
    }

    @Override
    public String getHostname() {
        return null;
    }

    @Override
    public Properties getDebeziumProperties() {
        return null;
    }


    @Override
    public String getTables() {
        return null;
    }

    @Override
    public String getType() {
        return MysqlCdc.type;
    }

    @Override
    public String getPrefix() {
        return null;
    }

    @Override
    public Properties getSinkProp() {
        return null;
    }

    @Override
    public int getTimePrecision() {
        return 0;
    }

    @Override
    public int getTimeZone() {
        return 0;
    }

    @Override
    public String getSinkEndTimeName() {
        return null;
    }

    @Override
    public Long getCheckpointing() {
        return 0L;
    }

    private MysqlCdc(String sql, String idParas) {
        loadTableSchema(idParas);
        parseConfig(sql);
    }

    public static MysqlCdc getInstance(String sql, String idParas) {
        return new MysqlCdc(sql, idParas);
    }

    @Override
    public DataStream<String> addSource(StreamExecutionEnvironment environment) {
        MySqlSource<String> oracleSource = MySqlSource.<String>builder()
                .hostname(this.getHostname())
                .port(this.getPort())
                .databaseList(this.getDataBase().split(","))
                .tableList(this.getTables().split(","))
                .username(this.getUserName())
                .password(this.getPassword())
                .serverTimeZone("UTC")  //时区
                .debeziumProperties(this.getDebeziumProperties())
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();
       return environment.fromSource(oracleSource, WatermarkStrategy.noWatermarks(), "mysql-source");
    }
}
