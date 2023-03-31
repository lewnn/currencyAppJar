package com.app.convert;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;

public class DorisConvertValue {
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static Object convertValue(Object value, DataType logicalType, int timePrecision, int timeZone) {
        if (value == null) {
            return null;
        }
        if (logicalType.getLogicalType() instanceof VarCharType) {
            return StringData.fromString(value.toString());
        } else if (logicalType.getLogicalType() instanceof DateType) {
            return StringData.fromString(Instant.ofEpochMilli((long) value).atZone(ZoneId.systemDefault()).toLocalDate().toString());
        } else if (logicalType.getLogicalType() instanceof TimestampType) {
            int num;
            if (timePrecision == 11) {
                num = 1000 * 1000;
            } else{
                num = 1000;
            }
            return TimestampData.fromTimestamp(Timestamp.valueOf(LocalDateTime.ofEpochSecond((long) value / num, 0, ZoneOffset.ofHours(timeZone))));
        } else if (logicalType.getLogicalType() instanceof DecimalType) {
            final DecimalType decimalType = ((DecimalType) logicalType.getLogicalType());
            return DecimalData.fromBigDecimal(new BigDecimal(value.toString()), decimalType.getPrecision(), decimalType.getScale());
        } else {
            return value;
        }
    }

    /***
     * @Description: 格式化时间 TIME
     * @Author: lcg
     * @Date: 2023/3/22 9:17
     */
    public static String formatTime(LocalDateTime date) {
        return TIME_FORMATTER.format(date);
    }

    /***
     * @Description: 格式化日期 DATE
     * @Author: lcg
     * @Date: 2023/3/22 9:17
     */
    public static String formatDate(LocalDateTime date) {
        return DATE_FORMATTER.format(date);
    }
}
