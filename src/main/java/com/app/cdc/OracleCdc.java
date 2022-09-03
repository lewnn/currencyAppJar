package com.app.cdc;

import com.app.constant.CdcConstant;
import java.util.Properties;

public class OracleCdc extends BaseCdc {
    public static String type= "oracle-cdc";

    @Override
    public String getConnect() {
        return cdcProper.getProperty("connector");
    }

    @Override
    public String getHostname() {
        return cdcProper.getProperty("hostname");
    }

    @Override
    public Properties getDebeziumProperties() {
        Properties properties = new Properties();
        for (Object o : cdcProper.keySet()) {
            String  proKey = o.toString();
            if(proKey.startsWith(CdcConstant.DEBEZIUM)){
                properties.setProperty(proKey.replace("debezium.",""), cdcProper.getProperty(proKey));
            }
        }
        return properties;
    }

    @Override
    public int getPort() {
        return Integer.parseInt(cdcProper.getProperty("port"));
    }

    @Override
    public String getUserName() {
        return cdcProper.getProperty("username");
    }

    @Override
    public String getPassword() {
        return  cdcProper.getProperty("password");
    }

    @Override
    public String getDataBase() {
        return  cdcProper.getProperty("schema-name");
    }

    //orcl
    @Override
    public String getDataBaseName() {
        return cdcProper.getProperty("database-name");
    }

    @Override
    public String getTables() {
        return cdcProper.getProperty("table-name");
    }

    @Override
    public String getType() {
        return OracleCdc.type;
    }


    private OracleCdc(String sql, String idParas) {
        parseConfig(sql);
        loadTableSchema(idParas);
    }

    public static OracleCdc getInstance(String sql, String idParas){
        return new OracleCdc(sql, idParas);
    }
}