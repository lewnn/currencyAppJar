package com.app.entity;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;

import java.io.Serializable;

public class DataTypeProcess implements Serializable {
    private String name;
    private String type; //类型
    private Integer precision; //长度
    private Integer scale; //精度

    public DataTypeProcess(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public DataTypeProcess setPrecisionAndScale(Integer precision, Integer scale) {
        this.precision = precision;
        this.scale = scale;
        return this;
    }


    public String getName() {
        return this.name;
    }

    public Integer getPrecision() {
        return this.precision;
    }

    enum OracleDataTypeEnum {
        VARCHAR("STRING", new VarCharType(255)),
        STRING("STRING", new VarCharType(255)),
        VARCHAR2("STRING", new VarCharType(255)),
        TIMESTAMP("TIMESTAMP", new TimestampType()),
        DATETIME("DATETIME", new TimestampType()),
        DATE("TIMESTAMP", new TimestampType()),
        DECIMAL("DECIMAL", new DecimalType()),
        NUMBER("INT", new IntType());
        String type;
        LogicalType logicalType;

        OracleDataTypeEnum(String type, LogicalType logicalType) {
            this.type = type;
            this.logicalType = logicalType;
        }

        private OracleDataTypeEnum getType(DataTypeProcess dataType) {
            for (OracleDataTypeEnum value : OracleDataTypeEnum.values()) {
                if (value.name().contains(dataType.type.toUpperCase())
                        || value.type.contains(dataType.type.toUpperCase())) {
                    return value;
                }
            }
            return null;
        }

        public LogicalType getData(DataTypeProcess dataType) {
            switch (getType(dataType)) {
                case STRING:
                case VARCHAR:
                case VARCHAR2:
                    return new VarCharType(dataType.precision);
                case TIMESTAMP:
                case DATE:
                case DATETIME:
                    return DATETIME.logicalType;
                case DECIMAL:
                case NUMBER:
                    if (dataType.scale != null) {
                        return new DecimalType(dataType.precision, dataType.scale);
                    } else {
                        return new DecimalType();
                    }
                default:
                    return new VarCharType(255);
            }

        }

        public DataType getEnumsData(DataTypeProcess dataType) {
            switch (getType(dataType)) {
                case STRING:
                case VARCHAR:
                case VARCHAR2:
                    return DataTypes.VARCHAR(dataType.precision);
                case TIMESTAMP:
                case DATE:
                case DATETIME:
                    return DataTypes.TIMESTAMP();
                case DECIMAL:
                case NUMBER:
                    return DataTypes.DECIMAL(dataType.precision, dataType.scale == null ? 0 : dataType.scale);
                default:
                    return DataTypes.VARCHAR(255);
            }

        }
    }

    /**
     * @author lcg
     * @operate 获取列类型的数据类型
     * @date 2022/9/5 15:00
     */
    public LogicalType getDataLogicalType() {
        return OracleDataTypeEnum.DATETIME.getData(this);
    }

    public DataType getDataType() {
        return OracleDataTypeEnum.DATETIME.getEnumsData(this);
    }
}
