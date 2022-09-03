package com.app.entity;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DataType implements Serializable {
    private  String name;
    private String type; //类型
    private Integer precision; //长度
    private Integer scale; //精度
    private Boolean keyInfo; //精度

    public DataType(String name, String type, Integer pk) {
        this.name = name;
        this.type = type;
        this.keyInfo = pk != null && pk == 1;
    }

    public DataType setPrecisionAndScale(Integer precision, Integer scale) {
        this.precision = precision;
        this.scale = scale;
        return this;
    }

    public boolean checkTimeStamp() {
        return this.type.toUpperCase().contains(OracleDataTypeEnum.TIMESTAMP.name())
                || this.type.toUpperCase().contains(OracleDataTypeEnum.DATETIME.name());
    }

    public String getName() {
        return this.name;
    }

    enum OracleDataTypeEnum {
        VARCHAR("string"),
        TIMESTAMP("string"),
        DATETIME("string"),
        NUMBER("int"),
        NUMBER_P_S("DECIMAL");
        String type;

        OracleDataTypeEnum(String type) {
            this.type = type;
        }

        public void getData() {
            OracleDataTypeEnum oracleDataTypeEnum = OracleDataTypeEnum.valueOf("");
            for (OracleDataTypeEnum value : OracleDataTypeEnum.values()) {
                value.toString().contains("");
            }
        }

    }

    private String getData(String col) {
        return "";
    }

    public Boolean getKeyInfo() {
        return keyInfo;
    }

    public static String getDateStr(String time) {
        int num = 1;
        if (time.length() >= 14) {
            num = 1000 * 1000;
        } else if (time.length() >= 11) {
            num = 1000;
        }
        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(Long.valueOf(time) / num, 0, ZoneOffset.ofHours(8));
        return localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"));
    }

    public static String getDateStr(String time, int zone) {
        int num = 1;
        if (time.length() >= 14) {
            num = 1000 * 1000;
        } else if (time.length() >= 11) {
            num = 1000;
        }
        LocalDateTime localDateTime = LocalDateTime.ofEpochSecond(Long.valueOf(time) / num, 0, ZoneOffset.ofHours(zone));
        return localDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"));
    }

    public static String getTableName(String idf, String prefix) {
        String[] idfs = idf.split("\\.");
        return prefix + idfs[idfs.length - 1];
    }

}
