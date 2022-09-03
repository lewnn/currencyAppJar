package com.app.func;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.RowData;

import java.util.Map;


public class CdcMapParseFunc implements MapFunction<String, RowData> {
    @Override
    public RowData map(String msg) throws Exception {

        return null;
    }


}
