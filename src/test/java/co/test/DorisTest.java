//package co.test;
//
//import org.apache.doris.flink.cfg.DorisExecutionOptions;
//import org.apache.doris.flink.cfg.DorisOptions;
//import org.apache.doris.flink.cfg.DorisReadOptions;
//import org.apache.doris.flink.cfg.DorisSink;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.DataTypes;
//import org.apache.flink.table.data.GenericRowData;
//import org.apache.flink.table.data.RowData;
//import org.apache.flink.table.data.StringData;
//import org.apache.flink.table.types.DataType;
//
//import java.util.Properties;
//
//public class DorisTest {
//    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // enable checkpoint
//        env.enableCheckpointing(10000);
//
//        DorisSink.Builder<RowData> builder = DorisSink.builder();
//
//        //doris sink option
//        DorisSink.Builder<RowData> builder = DorisSink.builder();
//
//        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
//        dorisBuilder.setFenodes("FE_IP:8030")
//                .setTableIdentifier("db.table")
//                .setUsername("root")
//                .setPassword("password");
//
//// json format to streamload
//        Properties properties = new Properties();
//        properties.setProperty("format", "json");
//        properties.setProperty("read_json_by_line", "true");
//        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
//        executionBuilder.setStreamLoadProp(properties); //streamload params
//
////flink rowdata‘s schema
//        String[] fields = {"city", "longitude", "latitude"};
//        DataType[] types = {DataTypes.VARCHAR(256), DataTypes.DOUBLE(), DataTypes.DOUBLE()};
//
//        builder.setDorisReadOptions(DorisReadOptions.builder().build())
//                .setDorisExecutionOptions(executionBuilder.build())
//                .setSerializer(RowDataSerializer.builder()    //serialize according to rowdata
//                        .setFieldNames(fields)
//                        .setType("json")           //json format
//                        .setFieldType(types).build())
//                .setDorisOptions(dorisBuilder.build());
//
////mock rowdata source
//        DataStream<RowData> source = env.fromElements("")
//                .map(new MapFunction<String, RowData>() {
//                    @Override
//                    public RowData map(String value) throws Exception {
//                        GenericRowData genericRowData = new GenericRowData(3);
//                        genericRowData.setField(0, StringData.fromString("beijing"));
//                        genericRowData.setField(1, 116.405419);
//                        genericRowData.setField(2, 39.916927);
//                        return genericRowData;
//                    }
//                });
//
//        source.sinkTo(builder.build());
//    }
//}
