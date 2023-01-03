package com.app.config;

import com.app.enums.DorisVersionEnum;

public abstract class ExcutorConfig {
    /**
     * mysql的ip
     */
    public final static  String MYSQL_IP = "10.1.51.25";
//    public final static  String MYSQL_IP = "10.10.10.206"; //东阿
//    public final static  String MYSQL_IP = "10.10.21.18"; //聊城二院

    public final static DorisVersionEnum DORIS_VERSION = DorisVersionEnum.V1_1_2;
    /**
     * 用户
     */
    public final static String MYSQL_USER = "dfly";
    /**
     * 用户
     */
    public final static String MYSQL_DIM_USER = "dim";
    /**
     * 用户
     */
    public final static String MYSQL_PORT = "3306";
    /**
     * 密码
     */
    public final static String MYSQL_PASSWORD = "dfly";
    /**
     * 驱动
     */
    public final static String DRIVER = "com.mysql.cj.jdbc.Driver";
    /**
     * URL
     */
    public final static String URL = "jdbc:mysql://" + MYSQL_IP + ":" + MYSQL_PORT+ "/" + MYSQL_USER + "?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&useSSL=false&zeroDateTimeBehavior=convertToNull&serverTimezone=Asia/Shanghai";
    /**
     *  DIM的URL
     */
    public final static String DIM_URL = "jdbc:mysql://" + MYSQL_IP + ":" + MYSQL_PORT+ "/" + MYSQL_DIM_USER + "?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&useSSL=false&zeroDateTimeBehavior=convertToNull&serverTimezone=Asia/Shanghai";


}
