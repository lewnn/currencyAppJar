package com.app.executor;

import com.app.MainApp;
import com.app.cdc.BaseCdc;
import com.app.entity.DataTypeProcess;
import com.app.func.FlatMapBuilder;
import com.app.sink.CusDorisSinkBuilder;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
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
    private StreamExecutionEnvironment environment;
    private volatile static ConcurrentHashMap<String, OutputTag<Map>> outputTagMap;

    public CdcExecutor(ConcurrentHashMap<String, OutputTag<Map>> outputTagMap) {
        CdcExecutor.outputTagMap = outputTagMap;
    }

    public void executeSql(BaseCdc baseCdc) throws Exception {
        Configuration config = baseCdc.getEnvConfig();
        config.set(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT, CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        environment = StreamExecutionEnvironment.getExecutionEnvironment(config);
        environment.enableCheckpointing(baseCdc.getCheckpointing());
        DataStream<String> startStream = baseCdc.addSource(environment);
        //旁路输出
        SingleOutputStreamOperator<HashMap> afterTag = process(startStream, baseCdc);
        //旁路输出 处理
        processTagAndAddSink(afterTag, baseCdc);

    }

    //旁路输出处理 &&  add sink
    private void processTagAndAddSink(SingleOutputStreamOperator<HashMap> afterTag, BaseCdc baseCdc) throws Exception {
        for (Map.Entry<String, OutputTag<Map>> stringOutputTagEntry : outputTagMap.entrySet()) {
            OutputTag<Map> data = stringOutputTagEntry.getValue();
            String id = data.getId();
            String sourceTableName = id.split("\\.")[1];
            String sinkTableName = baseCdc.getSinkTableName();
            String tableName = (!sinkTableName.isEmpty() ? sinkTableName : baseCdc.getPrefix() + sourceTableName).toUpperCase();
            //字段名称及数据类型
            List<DataTypeProcess> dataTypeInfo = MainApp.dataSchema.get(tableName);
            String[] field = getFields(dataTypeInfo, baseCdc);
            DataType[] types = getTypeList(dataTypeInfo, baseCdc);
            System.err.println("table---:" + id + " " + sourceTableName);
            afterTag.getSideOutput(data)
                    .flatMap(FlatMapBuilder.builder(baseCdc).setType(dataTypeInfo, types).setTableName(sourceTableName).build()).setParallelism(1)
                    .sinkTo(new CusDorisSinkBuilder().newDorisSink(tableName, baseCdc.getSinkProp(), field, types)).setParallelism(1).name(tableName);
        }
        environment.executeAsync(!baseCdc.getJobName().isEmpty() ? baseCdc.getJobName() : "CDC JOBS");
    }


    //数据格式处理 && 旁路输出
    private SingleOutputStreamOperator<HashMap> process(DataStream<String> startStream, BaseCdc baseCdc) {
        return startStream
                .map((MapFunction<String, HashMap>) value -> new ObjectMapper().readValue(value, HashMap.class)).returns(HashMap.class).setParallelism(1)
                .process(new ProcessFunction<HashMap, HashMap>() {
                    @Override
                    public void processElement(HashMap value, Context context, Collector<HashMap> collector) {
                        if (value.containsKey("source")) {
                            HashMap source = (HashMap) value.get("source");
                            if (baseCdc.checkSourceAndDb(source)) {
                                context.output(new OutputTag<Map>(baseCdc.getOutputTagName(source)) {
                                }, value);
                            }
                        }
                    }
                }).setParallelism(1);
    }

    /***
     * @Description: 获取字段个数及名称
     * @Author: lcg
     * @Date: 2023/3/17 15:54
     */
    private String[] getFields(List<DataTypeProcess> dataTypeInfo, BaseCdc baseCdc) {
        List<String> fieldList = new ArrayList<>();
        for (DataTypeProcess dataType : dataTypeInfo) {
            fieldList.add(dataType.getName());
        }
        if (baseCdc.isOpenChain()) {
            fieldList.addAll(baseCdc.getSinkTimedNameList());
        }
        return  fieldList.toArray(new String[0]);
    }


    /***
     * @Description: 获取字段的类型
     * @Author: lcg
     * @Date: 2023/3/17 15:56
     */
    private DataType[] getTypeList(List<DataTypeProcess> dataTypeInfo, BaseCdc baseCdc) {
        List<DataType> typeList = new ArrayList<>();
        for (DataTypeProcess dataType : dataTypeInfo) {
            typeList.add(dataType.getDataType());
        }
        if (baseCdc.isOpenChain()) {
            for (String ignored : baseCdc.getSinkTimedNameList()) {
                //默认 自动添加字段为String类型  适配Datetime类型 适配Date类型
                typeList.add(DataTypes.VARCHAR(30));
            }
        }
        return typeList.toArray(new DataType[0]);
    }

}