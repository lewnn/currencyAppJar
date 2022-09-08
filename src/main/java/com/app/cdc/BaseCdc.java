package com.app.cdc;

import com.app.utils.ExecuteSqlProcess;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class BaseCdc {
    public static String type = "base";

    protected Properties cdcProper = new Properties();

    public abstract String getConnect();

    public abstract int getPort();

    public abstract String getUserName();

    public abstract String getPassword();

    public abstract String getDataBase();

    public abstract String getDataBaseName();

    public abstract String getHostname();

    public abstract Properties getDebeziumProperties();

    public abstract String getTables();

    public abstract String getType();

    public abstract String getPrefix();

    public abstract Properties getSinkProp();

    public abstract int getTimePrecision();

    public abstract int getTimeZone();

    protected void loadTableSchema(String idParas) {
        ExecuteSqlProcess.loadSchema(idParas);
    }

    public abstract String getSinkEndTimeName();

    public abstract Long getCheckpointing();

    public void parseConfig(String sql) {
        String property = sql.substring(sql.indexOf("with") + 4).replace("(", "").replace(")", "");
        String[] splitSql = property.split("=");
        String matchStr = "\'\\S+\'";
        Pattern pattern = Pattern.compile(matchStr);
        List<String> propertyList = new ArrayList<>();
        for (String sqlStrSplit : splitSql) {
            Matcher matcher = pattern.matcher(sqlStrSplit);
            while (matcher.find()) {
                propertyList.add(matcher.group());
            }
        }
        for (int i = 0; i < propertyList.size(); i = i + 2) {
            cdcProper.setProperty(propertyList.get(i).replace("'", "")
                    , propertyList.get(i + 1).replace("'", ""));
        }
    }

    public static BaseCdc getInstance(String sql, String idParas) {
        if (sql.toUpperCase().contains(MysqlCdc.type.toUpperCase())) {
            return MysqlCdc.getInstance(sql, idParas);
        } else if (sql.toUpperCase().contains(OracleCdc.type.toUpperCase())) {
            return OracleCdc.getInstance(sql, idParas);
        } else {
            throw new RuntimeException("获取cdc数据库类型失败");
        }
    }


    public abstract DataStream<String> addSource(StreamExecutionEnvironment environment);
}
