package com.app.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class ReplaceWarp extends ScalarFunction {
    public String eval(String s) {
        return s.replace("\n","").replace("/r","");
    }
}
