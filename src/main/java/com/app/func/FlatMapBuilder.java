package com.app.func;

import com.app.cdc.BaseCdc;
import com.app.cdc.MysqlCdc;
import com.app.entity.DataTypeProcess;
import com.app.enums.ZipVersionEnum;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Map;

public class FlatMapBuilder {
    List<DataTypeProcess> dataTypeInfo;
    DataType[] types;
    BaseCdc baseCdc;
    String tableName;

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

    public FlatMapBuilder setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public FlatMapFunction<Map, RowData> build() {
        if (dataTypeInfo == null || types == null) {
            throw new RuntimeException("创建失败");
        }
        ZipVersionEnum zipVersion = baseCdc.getZipVersion();
        if (zipVersion.isOpenChain()) {
            Map<String, String> updateTimeColName = baseCdc.getUpdateTimeColName();
            String colName = updateTimeColName.get(tableName);
            switch (zipVersion) {
                //V1版本  只有一个字段 失效日期
                case V1:
                    return new ChainFlatMapFunc(dataTypeInfo,
                            types,
                            baseCdc.getTimePrecision(),
                            baseCdc.getTimeZone());
                //V2 产出两个字段 生效时间-失效时间
                case V_TIME:
                    return new ChainFlatMapFuncV2(dataTypeInfo,
                            types,
                            baseCdc.getTimePrecision(),
                            baseCdc.getTimeZone(),
                            baseCdc.getDebeziumTimeZone(),
                            colName);
                //V2 产出两个字段 生效日期-失效日期
                case V_DATE:
                    return new ChainFlatMapFuncV3(dataTypeInfo,
                            types,
                            baseCdc.getTimePrecision(),
                            baseCdc.getTimeZone(),
                            baseCdc.getDebeziumTimeZone(),
                            colName);
                default:
                    throw new RuntimeException("cdc拉链操作没有找到匹配的MapFunc");
            }

        } else {
            if (MysqlCdc.type.equals(baseCdc.getType())) {
                return new CusMysqlFlatMapFunc(dataTypeInfo,
                        types,
                        baseCdc.getTimePrecision(),
                        baseCdc.getTimeZone());
            }
            return new CusFlatMapFunc(dataTypeInfo,
                    types,
                    baseCdc.getTimePrecision(),
                    baseCdc.getTimeZone());
        }
    }

}
