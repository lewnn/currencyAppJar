package com.app.executor;

import com.app.MainApp;
import com.app.cdc.BaseCdc;
import com.app.cdc.MysqlCdc;
import com.app.cdc.OracleCdc;
import com.app.entity.DataTypeProcess;
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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CdcExecutor implements Serializable {
    private final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
    private volatile static ConcurrentHashMap<String, OutputTag<Map>> outputTagMap;

    public CdcExecutor(ConcurrentHashMap<String, OutputTag<Map>> outputTagMap) {
        this.outputTagMap = outputTagMap;
    }

    public void executeSql(BaseCdc baseCdc) throws Exception {
        environment.enableCheckpointing(20000);
        DataStream<String> startStream = null;
        startStream = baseCdc.addSource(environment);
        //旁路输出
        SingleOutputStreamOperator<HashMap> afterTag = process(startStream);
        //旁路输出 处理
        processTagAndAddSink( afterTag,baseCdc);

    }

    //旁路输出处理 &&  add sink
    private void processTagAndAddSink(SingleOutputStreamOperator<HashMap> afterTag, BaseCdc baseCdc) throws Exception {
        for (Map.Entry<String, OutputTag<Map>> stringOutputTagEntry : outputTagMap.entrySet()) {
            OutputTag<Map> data = stringOutputTagEntry.getValue();
            String id = data.getId();
            String tableName = baseCdc.getPrefix() + id.split("\\.")[1];
            //数据类型
            List<DataTypeProcess> dataTypeInfo = MainApp.dataSchema.get(tableName);
            List<String> fieldList = new ArrayList<>();
            List<DataType> typeList = new ArrayList<>();
            for (DataTypeProcess dataType : dataTypeInfo) {
                fieldList.add(dataType.getName());
                typeList.add(dataType.getDataType());
            }
            //field
            String[] field = fieldList.toArray(new String[fieldList.size() + 1]);
            DataType[] types = typeList.toArray(new DataType[typeList.size() + 1]);
            field[fieldList.size()] = baseCdc.getSinkEndTimeName();
            //field的属性
            types[fieldList.size()] = DataTypes.VARCHAR(30);
            afterTag.getSideOutput(data)
                    .flatMap(new ChainFlatMapFunc(dataTypeInfo, types, baseCdc.getTimePrecision(), baseCdc.getTimeZone())).setParallelism(1)
                    .sinkTo(new CusDorisSinkBuilder().newDorisSink(tableName, baseCdc.getSinkProp(), field, types)).setParallelism(1);
            environment.execute("cdc table " + tableName);
        }
    }


    //数据格式处理 && 旁路输出
    private SingleOutputStreamOperator<HashMap> process(DataStream<String> startStream) {
        return startStream
                .map(new MapFunction<String, HashMap>() {
                    @Override
                    public HashMap map(String value) throws Exception {
                        return new ObjectMapper().readValue(value, HashMap.class);
                    }
                }).setParallelism(1)
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
                }).setParallelism(1)
                .process(new ProcessFunction<HashMap, HashMap>() {
                    @Override
                    public void processElement(HashMap value, Context ctx, Collector<HashMap> out) throws Exception {
                        ctx.output(new OutputTag<Map>(value.get("idf").toString()) {
                        }, value);
                    }
                }).setParallelism(1);
    }


}