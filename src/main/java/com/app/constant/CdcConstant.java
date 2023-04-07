package com.app.constant;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public abstract class CdcConstant {
    public final static String DEBEZIUM = "debezium.";
    public final static String SINK_PROP = "sink.prop.";
    public final static String SINK = "sink.";
    public final static String FLINK_CONFIG = "flink.config.";
    public final static String CUS_CONFIG = "cus.";

    /***
     * 时间ms默认最大值 2999-01-01 00:00:00
     */
    public final static long MAX_TIME = 32472115200000L;
    /***
     * 时间ms默认最大值 String类型 2999-01-01 00:00:00
     */
    public final static String MAX_TIME_STR = "2999-01-01 00:00:00";

    /***
     * 时间ms默认最小值 1970-01-01 00:00:00
     */
    public final static long MIN_TIME = 0L;
    /***
     * 时间ms默认最小值 String类型 1970-01-01 00:00:00
     */
    public final static String MIN_TIME_STR = "1970-01-01 00:00:00";


    /***
     * 时间ms默认最大值 String类型 2999-01-01
     */
    public final static String MAX_DATE_STR = "2999-01-01";

    /***
     * 时间ms默认最小值 String类型 1970-01-01
     */
    public final static String MIN_DATE_STR = "1970-01-01";

    public final static List<String> CDC_PARA_LIST = Arrays.asList("connector", "hostname","port", "username", "password", "database-name", "schema-name", "table-name");
}
