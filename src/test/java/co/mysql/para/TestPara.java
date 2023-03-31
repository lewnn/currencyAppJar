package co.mysql.para;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

/**
 * Description:
 * User:lcg
 * Date:2023/3/6
 * TIME:11:26
 */
public class TestPara {

    @Test
    public void test(){
//        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createRemoteEnvironment("10.1.51.25",8081);
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment environment = StreamTableEnvironment.create(streamEnv);
        environment.executeSql("select TO_TIMESTAMP_LTZ(1678776461030,3),TO_TIMESTAMP('2023-11-19 11:13:25.123', 'yyyy-MM-dd HH:mm:ss.SSS')").print();
    }
}
