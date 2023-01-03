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
import org.apache.flink.types.RowKind;

import java.util.Properties;
import java.util.Random;

public class DorisSinkTest {
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
        properties.setProperty("columns", "city, pay, temp, __DORIS_DELETE_SIGN__");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setDeletable(true)//streamload label prefix
                .setLabelPrefix("test12333"+ new Random().nextInt(5000))
                .setStreamLoadProp(properties); //streamload params

//flink rowdata‘s schema
        String[] fields = {"city", "pay", "temp"};
        DataType[] types = {DataTypes.VARCHAR(256), DataTypes.DOUBLE(), DataTypes.DOUBLE()};

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(RowDataSerializer.builder()    //serialize according to rowdata
                        .setFieldNames(fields)
                        .setType("json")           //json format
                        .enableDelete(true)
                        .setFieldType(types).build())
                .setDorisOptions(dorisBuilder.build());


        DataStream<RowData> source = env.socketTextStream("10.1.51.26", 25561)
//        DataStream<RowData> source = env.fromElements("123","456","123")
                .map(new MapFunction<String, RowData>() {
                    Random random = new Random();
                    @Override
                    public RowData map(String value) throws Exception {
                        GenericRowData genericRowData = new GenericRowData(3);
                        if(value.equals("456")){
                            value = "123";
                            genericRowData.setRowKind(RowKind.INSERT);
                        }else if(value.equals("789")){
                            value = "123";
                            genericRowData.setRowKind(RowKind.DELETE);
                        }
                        genericRowData.setField(0, StringData.fromString("beijing"+ value) );
                        genericRowData.setField(1, Double.valueOf(value));
                        genericRowData.setField(2, Double.valueOf(value));
                        System.out.println(genericRowData);
                        return genericRowData;
                    }
                });

        source.sinkTo(builder.build());
        env.execute("1");
    }
}
