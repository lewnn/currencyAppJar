package com.app.executor;

import com.app.MainApp;
import com.app.cdc.BaseCdc;
import com.app.cdc.MysqlCdc;
import com.app.cdc.OracleCdc;
import com.app.entity.DataType;
import com.app.func.ChainFlatMapFunc;
import com.app.sink.CusDorisSinkBuilder;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CdcExecutor implements Serializable {
    private final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    private final static ConcurrentHashMap<String, OutputTag<Map>> outputTagMap = new ConcurrentHashMap<>();

    public void executeSql(String cdcList, String idParas) throws Exception {
        BaseCdc baseCdc = BaseCdc.getInstance(cdcList, idParas.split(",")[0]);
        DataStream<String> startStream = null;
        if (OracleCdc.type.equals(baseCdc.getType())) {
            SourceFunction<String> sourceFunction = OracleSource.<String>builder()
                    .hostname(baseCdc.getHostname())
                    .port(baseCdc.getPort())
                    .database(baseCdc.getDataBaseName())
                    .schemaList(baseCdc.getDataBase().split(","))
                    .tableList(baseCdc.getTables())
                    .username(baseCdc.getUserName())
                    .password(baseCdc.getPassword())
                    .debeziumProperties(baseCdc.getDebeziumProperties())
                    .deserializer(new JsonDebeziumDeserializationSchema())
                    .startupOptions(StartupOptions.latest())
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
                    .debeziumProperties(baseCdc.getDebeziumProperties())
                    .deserializer(new StringDebeziumDeserializationSchema())
                    .build();
            startStream = environment.fromSource(oracleSource, WatermarkStrategy.noWatermarks(), "mysql-source");
        }
        //table :  db.table
        for (String table : baseCdc.getTables().split(",")) {
            outputTagMap.put(table, new OutputTag<Map>(table) {
            });
        }

        //旁路输出
        SingleOutputStreamOperator<HashMap> afterTag = startStream
                .map(new MapFunction<String, HashMap>() {
                    @Override
                    public HashMap map(String value) throws Exception {
                        return new ObjectMapper().readValue(value, HashMap.class);
                    }
                })
                .filter((FilterFunction<HashMap>) value -> {
                    if (value.containsKey("source")) {
                        HashMap source = (HashMap) value.get("source");
                        if (source.containsKey("schema") && source.containsKey("table")) {
                            value.put("idf", source.get("schema") + "." + source.get("table"));
                            return true;
                        }
                        return false;
                    }
                    return false;
                })
                .process(new ProcessFunction<HashMap, HashMap>() {
                    @Override
                    public void processElement(HashMap value, Context ctx, Collector<HashMap> out) throws Exception {
                        ctx.output(outputTagMap.get(value.get("idf")), value);
                    }
                });


        for (Map.Entry<String, OutputTag<Map>> stringOutputTagEntry : outputTagMap.entrySet()) {
            OutputTag<Map> data = stringOutputTagEntry.getValue();
            String id = data.getId();
            String tableName = baseCdc.getPrefix() + id.split("\\.")[1];
            //数据类型
            List<DataType> dataTypeInfo = MainApp.dataSchema.get(tableName);
            List<String> fieldList = new ArrayList<>();
            List<LogicalType> typeList = new ArrayList<>();
            for (DataType dataType : dataTypeInfo) {
                fieldList.add(dataType.getName());
                typeList.add(dataType.getDataLogicalType());
            }
            //field
            String[] field = fieldList.toArray(new String[fieldList.size() + 1]);
            LogicalType[] types = typeList.toArray(new LogicalType[fieldList.size() + 1]);
            field[fieldList.size()] = baseCdc.getSinkEndTimeName();
            //field的属性
            types[fieldList.size()] = new VarCharType();
            afterTag.getSideOutput(data)
                    .flatMap(new ChainFlatMapFunc(dataTypeInfo, types, baseCdc.getTimePrecision(), baseCdc.getTimeZone()))
                    .addSink(new CusDorisSinkBuilder<RowData>().newDorisSink(tableName, baseCdc.getSinkProp(), field, types));
            environment.execute("tst");
        }
    }
}