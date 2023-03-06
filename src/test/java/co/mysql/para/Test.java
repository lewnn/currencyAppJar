package co.mysql.para;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Description:
 * User:lcg
 * Date:2023/3/6
 * TIME:11:26
 */
public class Test {

    @org.junit.Test
    public void test(){
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createRemoteEnvironment("10.1.51.25",8081);
        StreamTableEnvironment environment = StreamTableEnvironment.create(streamEnv);
        environment.executeSql("CREATE TABLE gtp ( `id` INTEGER,`name` STRING,`addr` STRING,`mail` STRING,PRIMARY KEY (id) NOT ENFORCED )WITH( 'connector'  = 'jdbc',  'url'   = 'jdbc:mysql://10.1.51.25:3306/test',  'username'   = 'dfly', 'table-name'  = 'gtp', 'password'  = 'dfly','data.filter'='id>1' )");
        TableResult ds = environment.executeSql(" select * from gtp ");
        ds.print();

    }
}
