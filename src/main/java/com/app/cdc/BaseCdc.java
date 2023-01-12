package com.app.cdc;

import com.app.constant.CdcConstant;
import com.app.utils.ExecuteSqlProcess;
import org.apache.doris.flink.table.DorisDynamicTableFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class BaseCdc implements Serializable {

    protected Properties cdcProper = new Properties();

    public String getConnect() {
        return cdcProper.getProperty("connector");
    }

    public int getPort() {
        return Integer.parseInt(cdcProper.getProperty("port"));
    }

    public String getUserName() {
        return cdcProper.getProperty("username");
    }

    public String getPassword() {
        return cdcProper.getProperty("password");
    }

    public String getSchemaName() {
        return "";
    }

    public String getDataBaseName() {
        return cdcProper.getProperty("database-name");
    }

    public String getHostname() {
        return cdcProper.getProperty("hostname");
    }

    public Properties getDebeziumProperties() {
        Properties properties = new Properties();
        for (Object o : cdcProper.keySet()) {
            String proKey = o.toString();
            if (proKey.startsWith(CdcConstant.DEBEZIUM)) {
                properties.setProperty(proKey.replace("debezium.", ""), cdcProper.getProperty(proKey));
            }
        }
        return properties;
    }

    public  String getTables(){
        return cdcProper.getProperty("table-name");
    }

    public abstract String getType();

    public abstract boolean checkSourceAndDb(HashMap source);

    public abstract String getOutputTagName(HashMap source);


    public  String getPrefix(){
        return cdcProper.getProperty("cus.table.prefix");
    }

    public  Properties getSinkProp(){
        Properties properties = new Properties();
        for (Object o : cdcProper.keySet()) {
            String proKey = o.toString();
            if (proKey.startsWith(CdcConstant.SINK_PROP)) {
                properties.setProperty(proKey.replace(CdcConstant.SINK_PROP, ""), cdcProper.getProperty(proKey));
            }
            if (proKey.startsWith(DorisDynamicTableFactory.STREAM_LOAD_PROP_PREFIX)) {
                properties.setProperty(proKey, cdcProper.getProperty(proKey));
            }
        }
        return properties;
    };

    public  int getTimePrecision(){
        return Integer.parseInt(cdcProper.getProperty("cus.time.precision", "1000"));
    }

    public  int getTimeZone(){
        return Integer.parseInt(cdcProper.getProperty("cus.time.zone", "0"));
    }

    public void loadTableSchema() {
        ExecuteSqlProcess.loadSchema(this);
    }

    public String getSinkEndTimeName() {
        return cdcProper.getProperty("chain.end.name");
    }

    public Long getCheckpointing() {
        return Long.valueOf(cdcProper.getProperty("cus.time.checkpointing"));
    }

    public Boolean isOpenChain() {
        return Boolean.valueOf(cdcProper.getProperty("cus.open.chain"));
    }

    public String getSinkTableName() {
        return String.valueOf(cdcProper.getOrDefault("sink.prop.cus.sink.table", ""));
    }


    public abstract String getTablesColumnsInfo(String db, String tableName);

    public String getJobName(){
        return cdcProper.getOrDefault("pipeline.name","").toString();
    }

    public Configuration getEnvConfig() {
        Configuration config = new Configuration();
        cdcProper.forEach((key, values) ->{
            String ksyStr = key.toString().replace(" ", "");
            if(ksyStr.startsWith(CdcConstant.FLINK_CONFIG)){
                config.setString(ksyStr.replace(CdcConstant.FLINK_CONFIG, ""), values.toString());
            }
        });
        return config;
    }

    public abstract DataStream<String> addSource(StreamExecutionEnvironment environment);


    public void parseConfig(String sql) {
        String property = sql.substring(sql.indexOf("with") + 4).replace("(", "").replace(")", "");
        String[] splitSql = property.split("=");
        String matchStr = "'\\S+'";
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
            return MysqlCdc.getInstance(sql);
        } else if (sql.toUpperCase().contains(OracleCdc.type.toUpperCase())) {
            return OracleCdc.getInstance(sql);
        } else {
            throw new RuntimeException("获取cdc数据库类型失败");
        }
    }


}
