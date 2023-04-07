package co;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

/**
 * Description:
 * User:lcg
 * Date:2023/4/6
 * TIME:17:20
 */
public class mysqlDDLTest {

    @Test
    public void test() throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.1.51.25")
                .port(3306)
                .databaseList("test") // 设置捕获的数据库， 如果需要同步整个数据库，请将 tableList 设置为 ".*".
                .tableList("*") // 设置捕获的表
                .username("dfly")
                .password("dfly")
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将 SourceRecord 转换为 JSON 字符串
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置 3s 的 checkpoint 间隔
        env.enableCheckpointing(3000);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // 设置 source 节点的并行度为 4
                .setParallelism(4)
                .print().setParallelism(1); // 设置 sink 节点并行度为 1

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
