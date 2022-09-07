package com.app.func;

import com.app.convert.DorisConvertValue;
import com.app.entity.DataType;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChainFlatMapFunc implements FlatMapFunction<Map, RowData> {
    List<DataType> dataTypeInfo;
    LogicalType[] types;
    int timePrecision;
    int timeZone;

    public ChainFlatMapFunc( List<DataType> dataTypeInfo, LogicalType[]types, int timePrecision, int timeZone){
        this.dataTypeInfo = dataTypeInfo;
       this.types = types;
       this.timePrecision = timePrecision;
       this.timeZone = timeZone;
    }
    @Override
    public void flatMap(Map value, Collector<RowData> out) throws Exception {
        GenericRowData newRow = new GenericRowData(dataTypeInfo.size() + 1);
        HashMap after = (HashMap) value.get("after");
        HashMap before = (HashMap) value.get("before");
        HashMap source = (HashMap) value.get("source");
        switch (value.get("op").toString()) {
            case "c":
                newRow.setRowKind(RowKind.INSERT);
                for (int i = 0; i < dataTypeInfo.size(); i++) {
                    newRow.setField(i , DorisConvertValue.convertValue(after.get(dataTypeInfo.get(i).getName().toUpperCase()), types[i], dataTypeInfo.get(i).getPrecision(), timeZone));
                }
                newRow.setField(dataTypeInfo.size(), StringData.fromString("2999-01-01 00:00:01"));
                out.collect(newRow);
                break;
            case "d":
                break;
            case "u":
                newRow.setRowKind(RowKind.UPDATE_BEFORE);
                for (int i = 0; i < dataTypeInfo.size(); i++) {
                    newRow.setField(i , DorisConvertValue.convertValue(before.get(dataTypeInfo.get(i).getName().toUpperCase()), types[i], dataTypeInfo.get(i).getPrecision(), timeZone));
                }
                newRow.setField(dataTypeInfo.size(), StringData.fromString("2999-01-01 00:00:01"));
                out.collect(newRow);
                newRow.setRowKind(RowKind.UPDATE_AFTER);
                newRow.setField(dataTypeInfo.size() , StringData.fromString(LocalDateTime.ofEpochSecond((long) source.get("ts_ms")/timePrecision, 0, ZoneOffset.ofHours(timeZone)).format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"))));
                out.collect(newRow);
                newRow.setRowKind(RowKind.UPDATE_AFTER);
                for (int i = 0; i < dataTypeInfo.size(); i++) {
                    newRow.setField(i , DorisConvertValue.convertValue(after.get(dataTypeInfo.get(i).getName().toUpperCase()), types[i], dataTypeInfo.get(i).getPrecision(), timeZone));
                }
                newRow.setField(dataTypeInfo.size(), StringData.fromString("2999-01-01 00:00:01"));
                out.collect(newRow);
                break;
        }
    }
}
