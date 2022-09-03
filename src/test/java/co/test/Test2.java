package co.test;

import java.util.HashMap;
import java.util.Map;

public class Test2 {
    public static void main(String[] args) throws Exception {
       String ds = "{\n" +
               "    'connector' = 'oracle-cdc',\n" +
               "    'hostname' = '10.1.51.25',\n" +
               "    'port' = '1521',\n" +
               "    'username' = 'cdr',\n" +
               "    'password' = 'cdr',\n" +
               "    'database-name' = 'orcl',\n" +
               "    'schema-name' = 'TEST',\n" +
               "    'table-name' = 'CDCTEST'\n" +
               "}";

    }


}
