package co.test;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.RowDataSerializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;

import java.util.Properties;
import java.util.Random;

public class TestNew {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(10000);

//doris sink option
        DorisSink.Builder<RowData> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("10.1.51.26:8030")
                .setTableIdentifier("test.test")
                .setUsername("root")
                .setPassword("dw123456");

// json format to streamload
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.disable2PC()//streamload label prefix
                .setStreamLoadProp(properties); //streamload params

//flink rowdata‘s schema
        String[] fields = {"city", "pay", "temp"};
        DataType[] types = {DataTypes.VARCHAR(256), DataTypes.DOUBLE(), DataTypes.DOUBLE()};

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(RowDataSerializer.builder()    //serialize according to rowdata
                        .setFieldNames(fields)
                        .setType("json")           //json format
                        .setFieldType(types).build())
                .setDorisOptions(dorisBuilder.build());


        DataStream<RowData> source = env.socketTextStream("10.1.51.26", 25561)
//        DataStream<RowData> source = env.fromElements("")
                .map(new MapFunction<String, RowData>() {
                    Random random = new Random();
                    @Override
                    public RowData map(String value) throws Exception {
                        GenericRowData genericRowData = new GenericRowData(3);
                        genericRowData.setField(0, StringData.fromString("beijing"+ random.nextInt(1000)) );
                        genericRowData.setField(1, random.nextDouble());
                        genericRowData.setField(2, random.nextDouble());
                        System.out.println(genericRowData);
                        return genericRowData;
                    }
                });

        source.sinkTo(builder.build());
        env.execute("");
    }
}
