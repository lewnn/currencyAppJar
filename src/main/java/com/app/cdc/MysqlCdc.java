package com.app.cdc;



import java.util.Properties;

public class MysqlCdc extends BaseCdc{
    public static String type= "mysql-cdc";
    @Override
    public String getConnect() {
        return null;
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public String getUserName() {
        return null;
    }

    @Override
    public String getPassword() {
        return null;
    }

    @Override
    public String getDataBase() {
        return null;
    }

    @Override
    public String getDataBaseName() {
        return null;
    }

    @Override
    public String getHostname() {
        return null;
    }

    @Override
    public Properties getDebeziumProperties() {
        return null;
    }


    @Override
    public String getTables() {
        return null;
    }

    @Override
    public String getType() {
        return MysqlCdc.type;
    }

    private MysqlCdc(String sql, String idParas) {
        loadTableSchema(idParas);
        parseConfig(sql);
    }

    public static MysqlCdc getInstance(String sql, String idParas){
        return new MysqlCdc(sql, idParas);
    }
}
