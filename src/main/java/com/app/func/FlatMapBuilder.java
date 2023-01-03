package com.app.func;

import com.app.cdc.BaseCdc;
import com.app.entity.DataTypeProcess;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Map;

public class FlatMapBuilder {
    List<DataTypeProcess> dataTypeInfo;
    DataType[] types;
    BaseCdc baseCdc;

    private FlatMapBuilder(BaseCdc baseCdc) {
        this.baseCdc = baseCdc;
    }

    public static FlatMapBuilder builder(BaseCdc baseCdc) {
        return new FlatMapBuilder(baseCdc);
    }

    public FlatMapBuilder setType(List<DataTypeProcess> dataTypeInfo,
                 DataType[] types) {
        this.dataTypeInfo = dataTypeInfo;
        this.types = types;
        return this;
    }

    public FlatMapFunction<Map, RowData> build() {
        if (dataTypeInfo == null || types == null) {
            throw new RuntimeException("创建失败");
        }
        if (baseCdc.isOpenChain()) {
            return new ChainFlatMapFunc(dataTypeInfo,
                    types,
                    baseCdc.getTimePrecision(),
                    baseCdc.getTimeZone());
        } else {
            return new CusFlatMapFunc(dataTypeInfo,
                    types,
                    baseCdc.getTimePrecision(),
                    baseCdc.getTimeZone());
        }
    }

}
