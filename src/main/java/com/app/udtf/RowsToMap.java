package com.app.udtf;


import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;



public  class RowsToMap extends TableAggregateFunction<String, Map> {
    @Override
    public Map createAccumulator() {
        return new HashMap();
    }


    public void accumulate(Map acc, String cls, Object v, String key) {
        String[] keys = key.split(",");
        for (int i = 0; i < keys.length; i++) {
            if (keys[i].equals(cls)) {
                acc.put(cls, v);
            }
        }
    }

    public void accumulate(Map acc, String cls, Object v) {
        acc.put(cls, v);
    }

    public void merge(Map acc, Iterable<Map> iterable) {
        for (Map otherAcc : iterable) {
            Iterator iter = otherAcc.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                accumulate(acc, entry.getKey().toString(), entry.getValue());
            }
        }
    }

    public void emitValue(Map acc, Collector<String> out) {
        out.collect(acc.toString());

    }
}