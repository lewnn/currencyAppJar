package co.ds;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Test {
    private  static  StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
    public  void test() throws Exception {
        DataStreamSource<String> stringDataStreamSource = see.readTextFile("E:\\idea\\javaproject\\enwchange\\src\\main\\java\\com\\lewcg\\enwchange\\utils\\city.txt");
        stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String s1 : s.split(" ")) {
                    collector.collect(Tuple2.of(s1,1));
                }
            }
        }).keyBy(x->x.f0).sum(1).print();
        see.execute("test");
    }
}
