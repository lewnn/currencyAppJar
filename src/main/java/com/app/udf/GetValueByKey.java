package com.app.udf;

import com.app.utils.ExecuteSqlProcess;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GetValueByKey extends ScalarFunction {
    private Map<String, Map<String, String>> defaultMap = new ConcurrentHashMap<>();

    public String eval(String keyInfo, String tableName, String codeColumn, String valueColumn) {
        return eval(keyInfo, tableName, codeColumn, valueColumn, null, null);
    }

    public String eval(String keyInfo, String tableName, String codeColumn, String valueColumn, String dictColumn, String dictValue) {
        Map<String, String> tableInfo = defaultMap.getOrDefault(tableName, new ConcurrentHashMap<>());
        if (tableInfo.isEmpty()) {
            tableInfo = loadTableInfo(tableName, dictValue, codeColumn, valueColumn, dictColumn);
            defaultMap.put(tableName, tableInfo);
        }
        return tableInfo.getOrDefault(keyInfo, "");
    }

    private Map<String, String> loadTableInfo(String tableName, String dictValue, String codeColumn, String valueColumn, String dictColumn) {
        return ExecuteSqlProcess.getCodeByValue(tableName, dictValue, codeColumn, valueColumn, dictColumn);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }


    @Override
    public void close() throws Exception {
        if (defaultMap != null) {
            defaultMap.clear();
        }
        super.close();
    }
}
