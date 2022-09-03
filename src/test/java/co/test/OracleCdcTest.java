package co.test;

import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;


public class OracleCdcTest {
    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.put("decimal.handling.mode", "string");
        SourceFunction<String> sourceFunction = OracleSource.<String>builder()
                .hostname("10.1.51.25")
                .port(1521)
                .database("ORCL") // monitor XE database
                .schemaList("TEST") // monitor inventory schema
                .tableList("TEST.CDC_TEST") // monitor products table
                .username("cdr")
                .password("cdr")
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
     /*   env.addSource(sourceFunction).map(new MapFunction<String, JsonObject>() {
            @Override
            public JsonObject map(String data) throws Exception {

                return JsonParser.parseString(data).getAsJsonObject();
            }
        })
                .filter(new FilterFunction<JsonObject>() {
                    @Override
                    public boolean filter(JsonObject data) throws Exception {
//                        System.out.println((data.has("before") && !data.get("before").isJsonNull()) + " mmm:" + data);
                        return data.has("before") && !data.get("before").isJsonNull();
                    }
                })
                .map((MapFunction<JsonObject, JsonObject>) data -> {
                   *//* if (!data.get("after").isJsonNull()) {
                        JsonObject after = data.getAsJsonObject("after");
                        System.out.println(after);
//                        Number price = after.get("PRICE").getAsNumber();
//                        System.out.println(">>:" + price.doubleValue());
                    }*//*
                    System.out.println(data);
                    return data;
                })
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
*/
        env.execute("Oracle CDC111");
    }
}
