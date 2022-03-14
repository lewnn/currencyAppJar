package com.app.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDate;

public class GetDayGap extends ScalarFunction {

    public int eval(String day) {
        if(day == null){
            return 0;
        }
        String[] split = day.split("-");
        if(split.length < 3){
            return 0;
        }
        LocalDate localDate = LocalDate.of(Integer.valueOf(split[0]), Integer.valueOf(split[1]), Integer.valueOf(split[2]));
        LocalDate startDay = LocalDate.of(1970, 1, 1);
        Long gap = localDate.toEpochDay() - startDay.toEpochDay();
        return  gap.intValue();
    }

}
