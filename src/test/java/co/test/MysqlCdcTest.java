package co.test;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;

public class MysqlCdcTest {
    public static void main(String[] args) throws Exception {

        //show binary logs;
        Properties prop = new Properties();
        prop.setProperty("snapshot.locking.mode", "none");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.1.51.25")
                .port(3306)
                .databaseList("test") // monitor all tables under inventory database
                .tableList("test.kafka_test") // set captured table
                .username("dfly")
                .password("dfly")
                .serverTimeZone("UTC")  //时区
                //设置读取位置 initial全量, latest增量,  specificOffset(binlog指定位置开始读,该功能新版本暂未支持)
                .debeziumProperties(prop)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//                createRemoteEnvironment("10.1.51.26", 8081);

        // enable checkpoint
        env.enableCheckpointing(3000);
        // set the source parallelism to 4
        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySqlParallelSource")
                .setParallelism(1)
                .map(x ->{
                    return x;
                })
                .print()
                .setParallelism(1);

        env.executeAsync("Print MySQL Snapshot + Binlog");
    }
}
