package com.app.udf;

import com.app.utils.ExecuteSqlProcess;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GetValueByKey extends ScalarFunction {
    private Map<String, Map<String, String>> defaultMap = new ConcurrentHashMap<>();

    public String eval(String keyInfo, String tableName, String codeColumn, String valueColumn) {
        Map<String, String> tableInfo = defaultMap.getOrDefault(tableName, new ConcurrentHashMap<>());
        if (tableInfo.isEmpty()) {
            tableInfo = loadTableInfo(tableName, codeColumn, valueColumn);
            defaultMap.put(tableName, tableInfo);
        }
        return tableInfo.getOrDefault(keyInfo, "");
    }


    private Map<String, String> loadTableInfo(String tableName, String codeColumn, String valueColumn) {
        return ExecuteSqlProcess.getCodeByValue(tableName, codeColumn, valueColumn);
    }


    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }


    @Override
    public void close() throws Exception {
        if(defaultMap != null){
            defaultMap.clear();
        }
        super.close();
    }
}
