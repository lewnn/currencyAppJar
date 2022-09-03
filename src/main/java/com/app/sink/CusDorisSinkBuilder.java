package com.app.sink;

import com.app.MainApp;
import com.app.entity.DataType;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class CusDorisSinkBuilder<T> {
    public SinkFunction<T> newDorisSink(String tableName){
        List<DataType> dataTypeInfo = MainApp.dataSchema.get(tableName);

        List<String> list = new ArrayList<>();
        for (DataType dataType : dataTypeInfo) {
            list.add(dataType.getName());
        }

        String[] field = list.toArray(new String[list.size() + 2]);
        field[3] = "s_dt";
        field[4] = "e_dt";
        LogicalType[] types = new LogicalType[field.length];
        types[0]= new  VarCharType();
        types[1]= new DecimalType(11,4);
        types[2]= new  VarCharType();
        types[3]= new VarCharType();
        types[4]= new  VarCharType();

        SinkFunction<T> sink = DorisSink.sink(
                field,
                types,
                DorisReadOptions.builder().build(),
                DorisExecutionOptions.builder()
                        .setBatchSize(3)
                        .setBatchIntervalMs(1L)
                        .setMaxRetries(3)
                        /*.setStreamLoadProp(pro)*/.build(),
                DorisOptions.builder()
                        .setFenodes("10.1.51.26:8030")
                        .setTableIdentifier("ods."+ tableName)
                        .setUsername("root")
                        .setPassword("dw123456").build()
        );
        return sink;
    }

}
