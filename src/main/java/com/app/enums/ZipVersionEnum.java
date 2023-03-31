package com.app.enums;

/**
 * Description:
 * User:lcg
 * Date:2023/3/17
 * TIME:15:25
 */
public enum ZipVersionEnum {
    V_NONE("none","默认值，不启用"),
    V1("v1","初始版本，此版本拉链只有一个失效日期"),
    V_TIME("TIME", "版本2，此版本要求有【更新时间】类型为Time，拉链后数据有生效日期和失效日期"),
    V_DATE("DATE", "版本3，此版本要求有【更新时间】类型为Date，拉链后数据有生效日期和失效日期");

    String version;
    String description;

    public boolean isOpenChain(){
        return this.version != null && !this.version.equalsIgnoreCase(V_NONE.version);
    }

    ZipVersionEnum(String version, String description) {
        this.version = version;
        this.description = description;
    }
    public static ZipVersionEnum getZipVersionByVersion(String version){
        if(version == null){
            return V_NONE;
        }
        for (ZipVersionEnum value : ZipVersionEnum.values()) {
            if(value.version.equalsIgnoreCase(version)){
                return value;
            }
        }
        return V_NONE;
    }

}
