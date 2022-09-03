package com.app.executor;

import com.app.MainApp;
import com.app.cdc.BaseCdc;
import com.app.cdc.MysqlCdc;
import com.app.cdc.OracleCdc;
import com.app.entity.DataType;
import com.app.sink.CusDorisSinkBuilder;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CdcExecutor implements Serializable {
    private final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

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
        ConcurrentHashMap<String, OutputTag<Map>> outputTagMap = new ConcurrentHashMap<>();
        for (String table : baseCdc.getTables().split(",")) {
            outputTagMap.put(table, new OutputTag<Map>(table) {
            });
        }
        SingleOutputStreamOperator<HashMap> afterTag = startStream
                .map(new MapFunction<String, HashMap>() {
                    @Override
                    public HashMap map(String value) throws Exception {
                        return new ObjectMapper().readValue(value, HashMap.class);
                    }
                })
                .filter((FilterFunction<HashMap>) value -> {
                    System.out.println("map>>" + value);
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
        LogicalType[] types = new LogicalType[5];
        types[0]= new  VarCharType();
        types[1]= new DecimalType(11,4);
        types[2]= new  VarCharType();
        types[3]= new VarCharType();
        types[4]= new  VarCharType();
        for (Map.Entry<String, OutputTag<Map>> stringOutputTagEntry : outputTagMap.entrySet()) {
            OutputTag<Map> data = stringOutputTagEntry.getValue();
            List<DataType> dataTypeInfo = MainApp.dataSchema.get(DataType.getTableName(data.getId(), "ODS_"));
            afterTag.getSideOutput(data).flatMap(new FlatMapFunction<Map, RowData>() {
                @Override
                public void flatMap(Map value, Collector<RowData> out) throws Exception {
                    GenericRowData newRow = new GenericRowData(dataTypeInfo.size() + 2);
                    HashMap after = (HashMap) value.get("after");
                    HashMap before = (HashMap) value.get("before");
                    switch (value.get("op").toString()) {
                        case "c":
                            newRow.setRowKind(RowKind.INSERT);
                            for (int i = 0; i < dataTypeInfo.size(); i++) {
                                newRow.setField(i , convertValue(after.get(dataTypeInfo.get(i).getName().toUpperCase()), types[i]));
                            }
                            newRow.setField(3 , StringData.fromString(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"))));
                            newRow.setField(4, StringData.fromString("2999-01-01 00:00:00"));
                            System.out.println(newRow);
                            out.collect(newRow);
                            break;
                        case "d":
                            break;
                        case "u":
                            newRow.setRowKind(RowKind.UPDATE_BEFORE);
                            for (int i = 0; i < dataTypeInfo.size(); i++) {
                                newRow.setField(i , convertValue(before.get(dataTypeInfo.get(i).getName().toUpperCase()), types[i]));
                            }
                            newRow.setField(3 , null);
                            newRow.setField(4, StringData.fromString("2999-01-01 00:00:00"));
                            out.collect(newRow);
                            newRow.setRowKind(RowKind.UPDATE_AFTER);
                            newRow.setField(4 , StringData.fromString(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"))));
                            out.collect(newRow);

                            newRow.setRowKind(RowKind.UPDATE_AFTER);
                            for (int i = 0; i < dataTypeInfo.size(); i++) {
                                newRow.setField(i , convertValue(after.get(dataTypeInfo.get(i).getName().toUpperCase()), types[i]));
                            }
                            newRow.setField(3 , StringData.fromString(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"))));
                            newRow.setField(4, StringData.fromString("2999-01-01 00:00:00"));
                            out.collect(newRow);
                            break;
                    }
                }
            }).addSink(new CusDorisSinkBuilder<RowData>().newDorisSink(DataType.getTableName(data.getId(), "ODS_")));
            environment.execute("tst");
        }
    }
    protected Object convertValue(Object value, LogicalType logicalType) {
        if (value == null) {
            return null;
        }
        if (logicalType instanceof VarCharType) {
            return StringData.fromString(value.toString());
        } else if (logicalType instanceof DateType) {
            return StringData.fromString(Instant.ofEpochMilli((long) value).atZone(ZoneId.systemDefault()).toLocalDate().toString());
        } else if (logicalType instanceof TimestampType) {
            return TimestampData.fromTimestamp(Timestamp.from(Instant.ofEpochMilli((long) value)));
        } else if (logicalType instanceof DecimalType) {
            final DecimalType decimalType = ((DecimalType) logicalType);
            final int precision = decimalType.getPrecision();
            final int scale = decimalType.getScale();
            return DecimalData.fromBigDecimal(new BigDecimal(value.toString() ), precision, scale);
        } else {
            return value;
        }
    }
}