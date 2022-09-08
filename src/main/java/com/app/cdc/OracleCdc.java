package com.app.cdc;

import com.app.constant.CdcConstant;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.doris.flink.table.DorisDynamicTableFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class OracleCdc extends BaseCdc {
    public static String type = "oracle-cdc";

    @Override
    public String getConnect() {
        return cdcProper.getProperty("connector");
    }

    @Override
    public String getHostname() {
        return cdcProper.getProperty("hostname");
    }

    @Override
    public Properties getDebeziumProperties() {
        Properties properties = new Properties();
        for (Object o : cdcProper.keySet()) {
            String proKey = o.toString();
            if (proKey.startsWith(CdcConstant.DEBEZIUM)) {
                properties.setProperty(proKey.replace("debezium.", ""), cdcProper.getProperty(proKey));
            }
        }
        return properties;
    }

    @Override
    public int getPort() {
        return Integer.parseInt(cdcProper.getProperty("port"));
    }

    @Override
    public String getUserName() {
        return cdcProper.getProperty("username");
    }

    @Override
    public String getPassword() {
        return cdcProper.getProperty("password");
    }

    @Override
    public String getDataBase() {
        return cdcProper.getProperty("schema-name");
    }

    //orcl
    @Override
    public String getDataBaseName() {
        return cdcProper.getProperty("database-name");
    }

    @Override
    public String getTables() {
        return cdcProper.getProperty("table-name");
    }

    @Override
    public String getType() {
        return OracleCdc.type;
    }

    @Override
    public String getPrefix() {
        return cdcProper.getProperty("cus.table.prefix");
    }

    @Override
    public Properties getSinkProp() {
        Properties properties = new Properties();
        for (Object o : cdcProper.keySet()) {
            String proKey = o.toString();
            if (proKey.startsWith(CdcConstant.SINK_PROP)) {
                properties.setProperty(proKey.replace(CdcConstant.SINK_PROP, ""), cdcProper.getProperty(proKey));
            }
            if (proKey.startsWith(DorisDynamicTableFactory.STREAM_LOAD_PROP_PREFIX)) {
                properties.setProperty(proKey, cdcProper.getProperty(proKey));
            }
        }
        return properties;
    }

    @Override
    public int getTimePrecision() {
        return Integer.parseInt(cdcProper.getProperty("cus.time.precision", "1000"));
    }

    @Override
    public int getTimeZone() {
        return Integer.parseInt(cdcProper.getProperty("cus.time.zone", "0"));
    }

    @Override
    public String getSinkEndTimeName() {
        return cdcProper.getProperty("chain.end.name");
    }

    @Override
    public Long getCheckpointing() {
        return Long.valueOf(cdcProper.getProperty("cus.time.checkpointing"));
    }


    private OracleCdc(String sql, String idParas) {
        parseConfig(sql);
        loadTableSchema(idParas);
    }

    public static OracleCdc getInstance(String sql, String idParas) {
        return new OracleCdc(sql, idParas);
    }

    @Override
    public DataStream<String> addSource(StreamExecutionEnvironment environment) {
        SourceFunction<String> sourceFunction = OracleSource.<String>builder()
                .hostname(this.getHostname())
                .port(this.getPort())
                .database(this.getDataBaseName())
                .schemaList(this.getDataBase().split(","))
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
