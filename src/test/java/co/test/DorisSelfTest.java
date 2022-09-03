/*
package co.test;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DorisSelfTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.milliseconds(30000)));
        DorisSink.Builder<String> builder = DorisSink.builder();
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
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("127.0.0.1:8040")
                .setTableIdentifier("db.table")
                .setUsername("test")
                .setPassword("test");
        DorisExecutionOptions.Builder  executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("label-doris")
                .setStreamLoadProp(properties)
                .setBufferSize(8*1024)
                .setBufferCount(3);

        builder.setDorisReadOptions(readOptionBuilder.build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(dorisBuilder.build());

        List<Tuple2<String, Integer>> data = new ArrayList<>();
        data.add(new Tuple2<>("doris",1));
        DataStreamSource<Tuple2<String, Integer>> source = env.fromCollection(data);
        source.map((MapFunction<Tuple2<String, Integer>, String>) t -> t.f0 + "," + t.f1)
                .sinkTo(builder.build());
        env.execute("doris test");
    }
}
*/
