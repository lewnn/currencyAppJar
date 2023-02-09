package com.app.constant;

public abstract class DataBaseConstant {
    /**
     * 秘钥
     */
    public static final String SECRET_KEY = "ubxGtbPpSfg=";

    /**
     * 数据源密码占位符
     */
    public static final String DB_NAME_TAG = "${%s-pw}";

    /**
     * 数据源密码占位符 不带${}
     */
    public static final String DB_NAME_BARE_TAG = "%s-pw";


    /**
     * 数据源连接信息占位符
     */
    public static final String DB_CONNECT_INFO = "${%s--ci}";
    /**
     * 数据源连接信息占位符 不带${}
     */
    public static final String DB_BARE_CONNECT_INFO = "%s--ci";
}
