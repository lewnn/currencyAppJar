package com.app.func;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;


public class CdcMapParseFunc2 implements MapFunction<String, Row> {
    @Override
    public Row map(String msg) throws Exception {
        return Row.of("");
    }


}
