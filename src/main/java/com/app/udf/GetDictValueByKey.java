package com.app.udf;

import com.app.constant.FlinkConstant;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GetDictValueByKey extends ScalarFunction {

    private HashSet<Tuple6<String, String, String, String, String, String>> allDataSet;
    private Map<String, String> mapForCodeCode = new ConcurrentHashMap<>();
    private Map<String, String> mapForCodeValue = new ConcurrentHashMap<>();
    private Map<String, String> mapForValueCode = new ConcurrentHashMap<>();
    private Map<String, String> mapForValueValue = new ConcurrentHashMap<>();

    public GetDictValueByKey(HashSet<Tuple6<String, String, String, String, String, String>> allDataSet) {
        this.allDataSet = allDataSet;
        parseDictData();
    }

    //            dict_version,source_code,source_value,source_dict,target_code,target_value
    //                       f0          f1          f2          f3          f4           f5
    //                       x_code, 'CHARGE_TYPE',      '2',             'V2V',              ''
    public String eval(String xPara, String xDict, String version, String type, String defaultValue) {
        if (this.allDataSet == null || this.allDataSet.isEmpty()) {
            return defaultValue;
        }
        if (FlinkConstant.CODE_CODE.equalsIgnoreCase(type)) {
            return mapForCodeCode.getOrDefault(xDict + version + xPara, defaultValue);
        } else if (FlinkConstant.CODE_VALUE.equalsIgnoreCase(type)) {
            return mapForCodeValue.getOrDefault(xDict + version + xPara, defaultValue);
        } else if (FlinkConstant.VALUE_CODE.equalsIgnoreCase(type)) {
            return mapForValueCode.getOrDefault(xDict + version + xPara, defaultValue);
        } else if (FlinkConstant.VALUE_VALUE.equalsIgnoreCase(type)) {
            return mapForValueValue.getOrDefault(xDict + version + xPara, defaultValue);
        }
        return defaultValue;
    }

    public String eval(String xPara, String xDict, String version, String type) {
        return eval(xPara, xDict, version, type, null);
    }

    /**
     * @return void
     * @author lcg
     * @operate 转换字典数据
     * @date 2021/9/17 9:39
     */
    //            dict_version,source_code,source_value,source_dict,target_code,target_value
    //                       f0          f1          f2          f3          f4           f5
    //                       x_code, 'CHARGE_TYPE',      '2',             'V2V',
    public void parseDictData() {
        if (mapForCodeCode.isEmpty()) {
            for (Tuple6<String, String, String, String, String, String> dictData : this.allDataSet) {
                mapForCodeCode.put(dictData.f3 + dictData.f0 + dictData.f1, dictData.f4);
            }
        }
        if (mapForCodeValue.isEmpty()) {
            for (Tuple6<String, String, String, String, String, String> dictData : this.allDataSet) {
                mapForCodeValue.put(dictData.f3 + dictData.f0 + dictData.f1, dictData.f5);
            }
        }
        if (mapForValueCode.isEmpty()) {
            for (Tuple6<String, String, String, String, String, String> dictData : this.allDataSet) {
                mapForValueCode.put(dictData.f3 + dictData.f0 + dictData.f2, dictData.f4);
            }
        }
        if (mapForValueValue.isEmpty()) {
            for (Tuple6<String, String, String, String, String, String> dictData : this.allDataSet) {
                mapForValueValue.put(dictData.f3 + dictData.f0 + dictData.f2, dictData.f5);
            }
        }

    }

    @Override
    public void close() throws Exception {
        if (allDataSet != null) {
            allDataSet.clear();
        }
        if (mapForCodeCode != null) {
            mapForCodeCode.clear();
        }
        if (mapForCodeValue != null) {
            mapForCodeValue.clear();
        }
        if (mapForValueCode != null) {
            mapForValueCode.clear();
        }
        if (mapForValueValue != null) {
            mapForValueValue.clear();
        }
        super.close();
    }
}
