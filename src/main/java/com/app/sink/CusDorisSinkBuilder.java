package com.app.sink;

import org.apache.doris.flink.cfg.*;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.RowDataSerializer;
import org.apache.doris.flink.table.DorisDynamicTableFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.Properties;
import java.util.Random;

public class CusDorisSinkBuilder {
    public DorisSink<RowData> newDorisSink(String tableName, Properties properties, String[] fields, DataType[] types) {
        if (fields == null || fields.length == 0) {
            throw new RuntimeException("获取表" + tableName + "字段信息为空");
        }
        String columns = String.join(",", fields) + ",__DORIS_DELETE_SIGN__";
        Properties steamLoadProp = getSteamLoadProp(properties);
        steamLoadProp.put("columns", columns);
        DorisSink.Builder<RowData> builder = DorisSink.builder();
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.disable2PC().setLabelPrefix(tableName + "_" + new Random().nextInt(100))
                .setStreamLoadProp(steamLoadProp).build();

        return builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(RowDataSerializer.builder()
                        .setFieldNames(fields)
                        .enableDelete(true)
                        .setType("json")
                        .setFieldType(types).build())
                .setDorisOptions(DorisOptions.builder().setFenodes(properties.getProperty(ConfigurationOptions.DORIS_FENODES))
                        .setTableIdentifier(properties.getProperty("cus.sink.db") + "." + tableName)
                        .setUsername(properties.getProperty(ConfigurationOptions.DORIS_USER))
                        .setPassword(properties.getProperty(ConfigurationOptions.DORIS_PASSWORD)).build()).build();

    }

    public Properties getSteamLoadProp(Properties properties) {
        Properties res = new Properties();
        properties.forEach((key, value) -> {
            if (key.toString().startsWith(DorisDynamicTableFactory.STREAM_LOAD_PROP_PREFIX)) {
                res.put(key.toString().replace(DorisDynamicTableFactory.STREAM_LOAD_PROP_PREFIX, ""), value);
            }
        });
        return res;
    }

}
