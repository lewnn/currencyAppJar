package com.app.executor;

import com.app.MainApp;
import com.app.cdc.BaseCdc;
import com.app.entity.DataTypeProcess;
import com.app.func.ChainFlatMapFunc;
import com.app.func.FlatMapBuilder;
import com.app.sink.CusDorisSinkBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
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
        CdcExecutor.outputTagMap = outputTagMap;
    }

    public void executeSql(BaseCdc baseCdc) throws Exception {
        environment.enableCheckpointing(20000);
        DataStream<String> startStream = baseCdc.addSource(environment);
        //旁路输出
        SingleOutputStreamOperator<HashMap> afterTag = process(startStream);
        //旁路输出 处理
        processTagAndAddSink(afterTag, baseCdc);

    }

    //旁路输出处理 &&  add sink
    private void processTagAndAddSink(SingleOutputStreamOperator<HashMap> afterTag, BaseCdc baseCdc) throws Exception {
        for (Map.Entry<String, OutputTag<Map>> stringOutputTagEntry : outputTagMap.entrySet()) {
            OutputTag<Map> data = stringOutputTagEntry.getValue();
            String id = data.getId();
            String sinkTableName = baseCdc.getSinkTableName();
            String tableName = !sinkTableName.isEmpty() ? sinkTableName : baseCdc.getPrefix() + id.split("\\.")[1];
            //字段名称及数据类型
            List<DataTypeProcess> dataTypeInfo = MainApp.dataSchema.get(tableName);
            String[] field = getFields(dataTypeInfo, baseCdc.getSinkEndTimeName());
            DataType[] types = getTypeList(dataTypeInfo);
            afterTag.getSideOutput(data)
                    .flatMap(FlatMapBuilder.builder(baseCdc).setType(dataTypeInfo, types).build()).setParallelism(1)
                    .sinkTo(new CusDorisSinkBuilder().newDorisSink(tableName, baseCdc.getSinkProp(), field, types)).setParallelism(1);
            environment.execute("cdc table " + tableName);
        }
    }


    //数据格式处理 && 旁路输出
    private SingleOutputStreamOperator<HashMap> process(DataStream<String> startStream) {
        return startStream
                .map((MapFunction<String, HashMap>) value -> new ObjectMapper().readValue(value, HashMap.class)).returns(HashMap.class).setParallelism(1)
                .process(new ProcessFunction<HashMap, HashMap>() {
                    @Override
                    public void processElement(HashMap value, Context context, Collector<HashMap> collector) {
                        if (value.containsKey("source")) {
                            HashMap source = (HashMap) value.get("source");
                            if (source.containsKey("schema") && source.containsKey("table")) {
                                context.output(new OutputTag<Map>(source.get("schema") + "." + source.get("table")) {
                                }, value);
                            }
                        }
                    }
                }).setParallelism(1);
    }

    private String[] getFields(List<DataTypeProcess> dataTypeInfo, String name) {
        List<String> fieldList = new ArrayList<>();
        for (DataTypeProcess dataType : dataTypeInfo) {
            fieldList.add(dataType.getName());
        }
        String[] field = fieldList.toArray(new String[fieldList.size() + 1]);
        field[fieldList.size()] = name;
        return field;
    }


    private DataType[] getTypeList(List<DataTypeProcess> dataTypeInfo) {
        List<DataType> typeList = new ArrayList<>();
        for (DataTypeProcess dataType : dataTypeInfo) {
            typeList.add(dataType.getDataType());
        }
        DataType[] types = typeList.toArray(new DataType[typeList.size() + 1]);
        //field的属性
        types[typeList.size()] = DataTypes.VARCHAR(30);
        return types;
    }


}