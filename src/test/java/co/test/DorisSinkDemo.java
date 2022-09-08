package co.test;

import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DorisSinkDemo {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DorisReadOptions.Builder readOptionBuilder = DorisReadOptions.builder();
        readOptionBuilder.setDeserializeArrowAsync(false)
                .setDeserializeQueueSize(64)
                .setExecMemLimit(2147483648L)
                .setRequestQueryTimeoutS(3600)
                .setRequestBatchSize(1000)
                .setRequestConnectTimeoutMs(10000)
                .setRequestReadTimeoutMs(10000)
                .setRequestRetries(3)
                .setRequestTabletSize(1024 * 1024);

        Properties properties = new Properties();
        properties.setProperty("column_separator", ",");
        properties.setProperty("line_delimiter", "\n");
        properties.setProperty("format", "csv");

        List<Tuple2<String, Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>("doris",1));
        DataStreamSource<Tuple2<String, Integer>> source = env.fromCollection(data);

    }
}
