package co.test;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.util.HashMap;

public class TestMysqlCdc {

    @Test
    public void test() throws JsonProcessingException {
        String tables = "test.a,test.b";
        for (String table : tables.split(",")) {
            if(table.contains(".")){
                table = table.split("\\.")[1];
            }
                System.out.println(table);
        }

    }

    @Test
    public void test2()  {
        String reg = "\\(\\d+\\)";
        String tables = "TIMESTAMP(6)";
        System.out.println(tables.replaceAll(reg, ""));
    }
    @Test
    public void test3()  {
        String ds = "SELECT    COLUMN_NAME AS 'col',DATA_TYPE AS 'type', " +
                "    CHARACTER_MAXIMUM_LENGTH AS 'varlen',NUMERIC_PRECISION AS 'numlen', " +
                "    NUMERIC_SCALE AS 'scale' " +
                "FROM  " +
                "    information_schema.`COLUMNS` " +
                "WHERE " +
                "    TABLE_SCHEMA =  '" + 1 + "'" +
                "    and TABLE_NAME= '" + 2 + "'" +
                "    ORDER by ORDINAL_POSITION";
        System.out.println(ds);
    }
}
