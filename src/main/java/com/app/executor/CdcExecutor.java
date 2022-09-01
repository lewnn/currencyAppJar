package com.app.executor;

import com.app.cdc.BaseCdc;
import com.app.cdc.MysqlCdc;
import com.app.cdc.OracleCdc;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CdcExecutor implements Serializable {
    private final  StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    public void executeSql(String cdcList, String idParas) throws Exception {
        BaseCdc baseCdc = BaseCdc.getInstance(cdcList, idParas.split(",")[0]);
        DataStream<String> startStream = null;
        if (OracleCdc.type.equals(baseCdc.getType())) {
            SourceFunction<String> sourceFunction = OracleSource.<String>builder()
                    .hostname(baseCdc.getHostname())
                    .port(baseCdc.getPort())
                    .database(baseCdc.getDataBaseName()) // monitor XE database
                    .schemaList(baseCdc.getDataBase().split(",")) // monitor inventory schema
                    .tableList(baseCdc.getTables()) // monitor products table
                    .username(baseCdc.getUserName())
                    .password(baseCdc.getPassword())
                    .debeziumProperties(baseCdc.getDebeziumProperties())
                    .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                    .build();
            startStream = environment.addSource(sourceFunction);
        } else if (MysqlCdc.type.equals(baseCdc.getType())) {
            MySqlSource<String> oracleSource = MySqlSource.<String>builder()
                    .hostname(baseCdc.getHostname())
                    .port(baseCdc.getPort())
                    .databaseList(baseCdc.getDataBase().split(","))
                    .tableList(baseCdc.getTables().split(","))
                    .username(baseCdc.getUserName())
                    .password(baseCdc.getPassword())
                    .serverTimeZone("UTC")  //时区
                    //设置读取位置 initial全量, latest增量,  specificOffset(binlog指定位置开始读,该功能新版本暂未支持)
                    .debeziumProperties(baseCdc.getDebeziumProperties())
                    .deserializer(new StringDebeziumDeserializationSchema())
                    .build();
            startStream = environment.fromSource(oracleSource, WatermarkStrategy.noWatermarks(), "mysql-source");
        }
        ConcurrentHashMap<String, OutputTag<JsonObject>> outputTagMap = new ConcurrentHashMap<>();
        for (String table : baseCdc.getTables().split(",")) {
            outputTagMap.put(table, new OutputTag<JsonObject>(table) {
            });
        }
        SingleOutputStreamOperator<JsonObject> afterTag = startStream
                .map(new MapFunction<String, JsonObject>() {
                    @Override
                    public JsonObject map(String value) throws Exception {
                        return JsonParser.parseString(value).getAsJsonObject();
                    }
                })
                .filter((FilterFunction<JsonObject>) value -> {
                    if (!value.isJsonNull() && !value.get("source").isJsonNull()) {
                        JsonObject source =  value.getAsJsonObject("source");
                        if (!source.get("schema").isJsonNull() && !source.get("table").isJsonNull()) {
                            System.out.println(value);
                            value.addProperty("idf", source.get("schema").getAsString().replace("\"", "")+"."+source.get("table").getAsString().replace("\"", ""));
                            return true;
                        }
                        return false;
                    }
                    return false;
                })
                .process(new ProcessFunction<JsonObject, JsonObject>() {
                    @Override
                    public void processElement(JsonObject value, Context ctx, Collector<JsonObject> out) throws Exception {
                        System.out.println("idf>>" + value.get("idf").getAsString());
                        ctx.output(outputTagMap.get(value.get("idf").getAsString()),value);
                    }
                });

        for (Map.Entry<String, OutputTag<JsonObject>> stringOutputTagEntry : outputTagMap.entrySet()) {
            OutputTag<JsonObject> data = stringOutputTagEntry.getValue();
            afterTag.getSideOutput(data).map(new MapFunction<JsonObject, JsonObject>() {
                @Override
                public JsonObject map(JsonObject value) throws Exception {
                    System.out.println(data.getId() + ">>" + value);
                    return value;
                }
            }).print();
        }
        environment.execute("tst");
    }
}
