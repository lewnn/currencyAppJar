package com.app.convert;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.*;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.*;

public class DorisConvertValue {

    public static Object convertValue(Object value, LogicalType logicalType, int timePrecision, int timeZone) {
        if (value == null) {
            return null;
        }
        if (logicalType instanceof VarCharType) {
            return StringData.fromString(value.toString());
        } else if (logicalType instanceof DateType) {
            return StringData.fromString(Instant.ofEpochMilli((long) value).atZone(ZoneId.systemDefault()).toLocalDate().toString());
        } else if (logicalType instanceof TimestampType) {
            int num = 1;
            if(timePrecision == 11){
                num = 1000 * 1000;
            }else if(timePrecision == 7){
                num = 1000;
            }
            return TimestampData.fromTimestamp(Timestamp.valueOf(LocalDateTime.ofEpochSecond((long)value/num, 0, ZoneOffset.ofHours(timeZone))));
        } else if (logicalType instanceof DecimalType) {
            final DecimalType decimalType = ((DecimalType) logicalType);
            return DecimalData.fromBigDecimal(new BigDecimal(value.toString() ), decimalType.getPrecision(), decimalType.getScale());
        } else {
            return value;
        }
    }
}
