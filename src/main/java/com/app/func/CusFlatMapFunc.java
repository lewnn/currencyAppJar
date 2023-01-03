package com.app.func;

import com.app.convert.DorisConvertValue;
import com.app.entity.DataTypeProcess;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CusFlatMapFunc implements FlatMapFunction<Map, RowData> {

    List<DataTypeProcess> dataTypeInfo;
    DataType[] types;
    int timePrecision;
    int timeZone;

    public CusFlatMapFunc(List<DataTypeProcess> dataTypeInfo, DataType[]types, int timePrecision, int timeZone){
        this.dataTypeInfo = dataTypeInfo;
       this.types = types;
       this.timePrecision = timePrecision;
       this.timeZone = timeZone;
    }
    @Override
    public void flatMap(Map value, Collector<RowData> out) {
        GenericRowData newRow = new GenericRowData(dataTypeInfo.size());
        HashMap after = (HashMap) value.get("after");
        HashMap before = (HashMap) value.get("before");
        switch (value.get("op").toString()) {
            case "c":
                newRow.setRowKind(RowKind.INSERT);
                for (int i = 0; i < dataTypeInfo.size(); i++) {
                    newRow.setField(i , DorisConvertValue.convertValue(after.get(dataTypeInfo.get(i).getName().toUpperCase()), types[i], dataTypeInfo.get(i).getPrecision(), timeZone));
                }
                out.collect(newRow);
                break;
            case "d":
                newRow.setRowKind(RowKind.DELETE);
                for (int i = 0; i < dataTypeInfo.size(); i++) {
                    newRow.setField(i , DorisConvertValue.convertValue(before.get(dataTypeInfo.get(i).getName().toUpperCase()), types[i], dataTypeInfo.get(i).getPrecision(), timeZone));
                }
                out.collect(newRow);
                break;
            case "u":
                newRow.setRowKind(RowKind.UPDATE_BEFORE);
                for (int i = 0; i < dataTypeInfo.size(); i++) {
                    newRow.setField(i , DorisConvertValue.convertValue(before.get(dataTypeInfo.get(i).getName().toUpperCase()), types[i], dataTypeInfo.get(i).getPrecision(), timeZone));
                }
                out.collect(newRow);
                newRow.setRowKind(RowKind.UPDATE_AFTER);
                for (int i = 0; i < dataTypeInfo.size(); i++) {
                    newRow.setField(i , DorisConvertValue.convertValue(after.get(dataTypeInfo.get(i).getName().toUpperCase()), types[i], dataTypeInfo.get(i).getPrecision(), timeZone));
                }
                out.collect(newRow);
                break;
        }
    }
}
