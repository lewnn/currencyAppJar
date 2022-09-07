package com.app.sink;

import org.apache.doris.flink.cfg.*;
import org.apache.doris.flink.table.DorisDynamicTableFactory;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.types.logical.LogicalType;
import java.util.Properties;


public class CusDorisSinkBuilder<T> {
    public SinkFunction<T> newDorisSink(String tableName, Properties properties,String[] field,  LogicalType[] types) {
        return DorisSink.sink(
                field,
                types,
                DorisReadOptions.builder().build(),
                DorisExecutionOptions.builder()
                        .setBatchSize(Integer.valueOf(properties.getProperty(ConfigurationOptions.DORIS_BATCH_SIZE, "1024")))
                        .setBatchIntervalMs(Long.valueOf(properties.getProperty("sink.batch.interval", "60000")))
                        .setMaxRetries(Integer.valueOf(properties.getProperty("sink.max-retries", "3")))
                        .setStreamLoadProp(getSteamLoadProp(properties)).build(),
                DorisOptions.builder()
                        .setFenodes(properties.getProperty(ConfigurationOptions.DORIS_FENODES))
                        .setTableIdentifier(properties.getProperty("cus.sink.db") + "." + tableName)
                        .setUsername(properties.getProperty(ConfigurationOptions.DORIS_USER))
                        .setPassword(properties.getProperty(ConfigurationOptions.DORIS_PASSWORD)).build()
        );
    }

    public Properties getSteamLoadProp(Properties properties){
        Properties res = new Properties();
        properties.forEach((key, value) ->{
            if(key.toString().startsWith(DorisDynamicTableFactory.STREAM_LOAD_PROP_PREFIX)){
                res.put(key, value);
            }
        });
        System.out.println(res);
        return res;
    }

}
