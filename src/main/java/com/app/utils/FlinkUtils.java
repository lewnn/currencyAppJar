package com.app.utils;

import com.app.MainApp;
import com.app.constant.FlinkConstant;
import com.app.udf.GetDictValueByKey;
import com.app.udf.GetKey;
import com.app.udtf.RowsToMap;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkUtils {

    /**
     * JSON字符串转Map
     */
    public static Map<String, Object> getMapFromJsonStr(String jsonStr) {
        Map<String, Object> map = new HashMap<>();
        if (jsonStr == null || jsonStr.isEmpty()) {
            return map;
        } else {
            jsonStr = jsonStr.replaceAll("\\{", "").replaceAll("\\}", "").replaceAll("(\\\r\\\n|\\\r|\\\n|\\\n\\\r)", "").replaceAll("\"", "");
            try {
                String[] splitStr = jsonStr.split(",");
                for (String msg : splitStr) {
                    String[] msgInfo = msg.split(":");
                    map.put(msgInfo[0], msgInfo[1]);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return map;
    }

    /**
     * Row TO String[]
     */
    public static String[] rowToString(Row row, String nullColumn, boolean printRowKind) {
        int len = printRowKind ? row.getArity() + 1 : row.getArity();
        List<String> fields = new ArrayList(len);
        if (printRowKind) {
            fields.add(row.getKind().shortString());
        }
        for (int i = 0; i < row.getArity(); ++i) {
            Object field = row.getField(i);
            if (field == null) {
                fields.add(nullColumn);
            } else {
                fields.add(StringUtils.arrayAwareToString(field));
            }
        }
        return (String[]) fields.toArray(new String[0]);
    }

    /**
     * @return void
     * @author lcg
     * @operate flink参数配置
     * @date 2021/9/16 10:24
     */
    public static void configEnvPara(Map<String, Object> mapFromJsonStr, StreamExecutionEnvironment streamEnv) {
        streamEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 保存点模式：exactly_once
        streamEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 触发保存点的时间间隔
        streamEnv.getCheckpointConfig().setCheckpointInterval(6000L);
        streamEnv.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        //设置间隙
        streamEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //设置超时时间
        streamEnv.getCheckpointConfig().setCheckpointTimeout(5 * 60 * 1000);
        //rocksdb 是否禁用
        if (mapFromJsonStr.containsKey(FlinkConstant.STATE_BACKEND_TYPE)) {
            Boolean rocksDBAbandon = Boolean.valueOf(mapFromJsonStr.get(FlinkConstant.STATE_BACKEND_TYPE).toString());
            System.out.println(LocalDateTime.now() + " rocksDBAbandon " + rocksDBAbandon);
            if (!rocksDBAbandon) {
                streamEnv.setStateBackend(new FsStateBackend(FlinkConstant.STATE_BACKEND_DIR));
            }
        }
        //并行度设置
        if (mapFromJsonStr.containsKey(FlinkConstant.PARALLELISM_NAME)) {
            Integer numParall = Integer.valueOf(mapFromJsonStr.get(FlinkConstant.PARALLELISM_NAME).toString());
            System.out.println(LocalDateTime.now() + " numParall " + numParall);
            streamEnv.setParallelism(numParall);
        }
    }

    /**
     * @return boolean
     * @author lcg
     * @operate 检验执行sql知否包含 某一个函数
     * @date 2021/9/16 10:41
     */
    public static boolean checkContainsOneFunction(List<String> listSql, String functionName) {
        for (String sqlText : listSql) {
            if (sqlText.toUpperCase().contains(functionName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return void
     * @author lcg
     * @operate 注册函数
     * @date 2021/9/16 10:57
     */
    public static void registerFunctionOfFlink(StreamTableEnvironment streamTableEnv, List<String> listSql) {
        ArrayList<String> udfList = FlinkConstant.UDF_LIST;
        for (String functionName : udfList) {
            switch (functionName) {
                case FlinkConstant.UDF_TO_DICT_MAPPING:
                    if (FlinkUtils.checkContainsOneFunction(listSql, FlinkConstant.UDF_TO_DICT_MAPPING)) {
                        ExecuteSqlProcess.getCodeOrValueFunctionData(getDictTypeForUdf(listSql));
                        System.out.println("加载函数" + FlinkConstant.UDF_TO_DICT_MAPPING);
                        streamTableEnv.createTemporarySystemFunction(FlinkConstant.UDF_TO_DICT_MAPPING, new GetDictValueByKey(MainApp.allDataSet));
                        MainApp.allDataSet = null;
                    }
                    break;
                case FlinkConstant.UDF_TO_MAP:
                    if (FlinkUtils.checkContainsOneFunction(listSql, FlinkConstant.UDF_TO_MAP)) {
                        System.out.println("加载函数" + FlinkConstant.UDF_TO_MAP);
                        streamTableEnv.registerFunction(FlinkConstant.UDF_TO_MAP, new RowsToMap());
                    }
                    break;
                case FlinkConstant.UDF_GET_KEY:
                    if (FlinkUtils.checkContainsOneFunction(listSql, FlinkConstant.UDF_GET_KEY)) {
                        System.out.println("加载函数" + FlinkConstant.UDF_GET_KEY);
                        streamTableEnv.createTemporarySystemFunction(FlinkConstant.UDF_GET_KEY, GetKey.class);
                    }
                    break;
            }
        }
    }


    /**
     *
     * @author lcg
     * @operate 获取字典类型
     * @date 2021/9/17 9:48
     * @return java.lang.String
     */
    public static String getDictTypeForUdf(List<String> listSql) {
        ArrayList<String> list = new ArrayList<>();
        try {
            for (String sql : listSql) {
                if (sql.toUpperCase().contains(FlinkConstant.UDF_TO_DICT_MAPPING)) {
                    int index = sql.indexOf(FlinkConstant.UDF_TO_DICT_MAPPING);
                    int indexLeft = sql.indexOf(FlinkConstant.BRACKET_LEFT, index);
                    int indexRight = sql.indexOf(FlinkConstant.BRACKET_RIGHT, index);
                    String sqlSub = sql.substring(indexLeft + 1, indexRight);
                    list.add(sqlSub.split(FlinkConstant.COMMA)[FlinkConstant.UDF_TO_DICT_MAPPING_DICT_INDEX]);
                }
            }
        } catch (Exception e) {
        }
        if (!list.isEmpty()) {
            return FlinkConstant.BRACKET_LEFT + String.join(FlinkConstant.COMMA, list) + FlinkConstant.BRACKET_RIGHT;
        }
        return "";
    }

}
