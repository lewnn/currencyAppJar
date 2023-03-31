package com.app.func;

import com.app.constant.CdcConstant;
import com.app.convert.DorisConvertValue;
import com.app.entity.DataTypeProcess;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChainFlatMapFuncV2 implements FlatMapFunction<Map, RowData> {
    List<DataTypeProcess> dataTypeInfo;
    DataType[] types;
    int timePrecision;
    int timeZone;
    int debeziumTimeZone;
    String updateTimeColName;

    public ChainFlatMapFuncV2(List<DataTypeProcess> dataTypeInfo,
                              DataType[] types,
                              int timePrecision,
                              int timeZone,
                              int debeziumTimeZone,
                              String updateTimeColName) {
        this.dataTypeInfo = dataTypeInfo;
        this.types = types;
        this.timePrecision = timePrecision;
        this.timeZone = timeZone;
        this.debeziumTimeZone = debeziumTimeZone;
        this.updateTimeColName = updateTimeColName;
    }

    @Override
    public void flatMap(Map value, Collector<RowData> out) {
        GenericRowData newRow = new GenericRowData(dataTypeInfo.size() + 2);
        HashMap after = (HashMap) value.get("after");
        HashMap before = (HashMap) value.get("before");
        HashMap source = (HashMap) value.get("source");
        System.err.println(">>" + value);
        switch (value.get("op").toString()) {
            case "r":
                newRow.setRowKind(RowKind.INSERT);
                for (int i = 0; i < dataTypeInfo.size(); i++) {
                    String colName = dataTypeInfo.get(i).getName();
                    newRow.setField(i, DorisConvertValue.convertValue(after.get(colName), types[i], dataTypeInfo.get(i).getPrecision(), timeZone));
                }
                newRow.setField(dataTypeInfo.size(), StringData.fromString(CdcConstant.MIN_TIME_STR));
                newRow.setField(dataTypeInfo.size() + 1, StringData.fromString(CdcConstant.MAX_TIME_STR));
                out.collect(newRow);
                break;
            case "c":
                newRow.setRowKind(RowKind.INSERT);
                for (int i = 0; i < dataTypeInfo.size(); i++) {
                    newRow.setField(i, DorisConvertValue.convertValue(after.get(dataTypeInfo.get(i).getName()), types[i], dataTypeInfo.get(i).getPrecision(), timeZone));
                }
                //新增的额数据，设置有效日期为    (最小,最大)
                newRow.setField(dataTypeInfo.size(), StringData.fromString(CdcConstant.MIN_TIME_STR));
                newRow.setField(dataTypeInfo.size() + 1, StringData.fromString(CdcConstant.MAX_TIME_STR));
                out.collect(newRow);
                break;
            case "d":
                newRow.setRowKind(RowKind.DELETE);
                for (int i = 0; i < dataTypeInfo.size(); i++) {
                    String colName = dataTypeInfo.get(i).getName();
                    newRow.setField(i, DorisConvertValue.convertValue(before.get(colName), types[i], dataTypeInfo.get(i).getPrecision(), timeZone));
                    if (colName.equalsIgnoreCase(updateTimeColName)) {
                        TimestampData updateTime = (TimestampData) DorisConvertValue.convertValue(before.get(colName), types[i], dataTypeInfo.get(i).getPrecision(), timeZone);
                        newRow.setField(dataTypeInfo.size(), StringData.fromString(DorisConvertValue.formatTime(updateTime.toLocalDateTime())));
                    }
                }
                newRow.setField(dataTypeInfo.size() + 1, StringData.fromString(CdcConstant.MAX_TIME_STR));
                out.collect(newRow);
                newRow.setRowKind(RowKind.INSERT);
                newRow.setField(dataTypeInfo.size() + 1,
                        StringData.fromString(
                                LocalDateTime.ofEpochSecond((long) source.get("ts_ms") / timePrecision,
                                        0,
                                        ZoneOffset.ofHours(debeziumTimeZone)
                                ).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));
                out.collect(newRow);
                break;
            case "u":
                //旧数据删除
                newRow.setRowKind(RowKind.UPDATE_BEFORE);
                for (int i = 0; i < dataTypeInfo.size(); i++) {
                    String colName = dataTypeInfo.get(i).getName();
                    newRow.setField(i, DorisConvertValue.convertValue(before.get(colName), types[i], dataTypeInfo.get(i).getPrecision(), timeZone));
                    if (colName.equalsIgnoreCase(updateTimeColName)) {
                        TimestampData updateTime = (TimestampData) DorisConvertValue.convertValue(
                                before.get(colName),
                                types[i],
                                dataTypeInfo.get(i).getPrecision(),
                                timeZone);
                        newRow.setField(dataTypeInfo.size(), StringData.fromString(DorisConvertValue.formatTime(updateTime.toLocalDateTime())));
                    }
                }
                newRow.setField(dataTypeInfo.size() + 1, StringData.fromString(CdcConstant.MAX_TIME_STR));
                out.collect(newRow);
                //旧数据修改失效时间 添加
                boolean endTimeIsSet = false;
                newRow.setRowKind(RowKind.UPDATE_AFTER);
                for (int i = 0; i < dataTypeInfo.size(); i++) {
                    String colName = dataTypeInfo.get(i).getName();
                    if (colName.equalsIgnoreCase(updateTimeColName)) {
                        TimestampData updateTime = (TimestampData) DorisConvertValue.convertValue(
                                after.get(colName),
                                types[i],
                                dataTypeInfo.get(i).getPrecision(),
                                timeZone);
                        newRow.setField(dataTypeInfo.size() + 1, StringData.fromString(DorisConvertValue.formatTime(updateTime.toLocalDateTime())));
                        endTimeIsSet = true;
                    }
                }
                if (!endTimeIsSet) {
                    newRow.setField(dataTypeInfo.size() + 1,
                            StringData.fromString(LocalDateTime
                                    .ofEpochSecond((long) source.get("ts_ms") / timePrecision,
                                            0,
                                            ZoneOffset.ofHours(debeziumTimeZone))
                                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));
                }
                out.collect(newRow);
                //新数据添加
                newRow.setRowKind(RowKind.UPDATE_AFTER);
                endTimeIsSet = false;
                for (int i = 0; i < dataTypeInfo.size(); i++) {
                    String colName = dataTypeInfo.get(i).getName();
                    newRow.setField(i, DorisConvertValue.convertValue(after.get(colName), types[i], dataTypeInfo.get(i).getPrecision(), timeZone));
                    if (colName.equalsIgnoreCase(updateTimeColName)) {
                        TimestampData updateTime = (TimestampData) DorisConvertValue.convertValue(
                                after.get(colName),
                                types[i],
                                dataTypeInfo.get(i).getPrecision(),
                                timeZone);

                        newRow.setField(dataTypeInfo.size(), StringData.fromString(DorisConvertValue.formatTime(updateTime.toLocalDateTime())));
                        endTimeIsSet = true;
                    }
                }
                if (!endTimeIsSet) {
                    newRow.setField(dataTypeInfo.size(), StringData.fromString(
                            LocalDateTime.ofEpochSecond(
                                    (long) source.get("ts_ms") / timePrecision,
                                    0,
                                    ZoneOffset.ofHours(debeziumTimeZone)
                            ).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));
                }
                newRow.setField(dataTypeInfo.size() + 1, StringData.fromString(CdcConstant.MAX_TIME_STR));
                out.collect(newRow);
                break;
        }
    }
}
