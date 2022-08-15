package com.app.utils;

import com.app.MainApp;
import com.app.config.MysqlConfig;
import com.app.constant.FlinkConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lcg
 * @operate 获取执行的sql
 * @date 2021/9/16 9:46
 */
public class ExecuteSqlProcess {
    private static final Logger logger = LoggerFactory.getLogger(ExecuteSqlProcess.class);

    /**
     * @return java.lang.String
     * @author lcg
     * @operate 获取sql
     * @date 2021/9/16 9:54
     */
    public static String getExecuteSql(String[] ids, List<String> flinkSqlList) throws IOException {
        String envConfig = "";
        HashMap<String, String> configMap = getExecuteSqlConfig();
        List<String> flinkSqlListTemp = new ArrayList<>();
        for (String id : ids) {
            if (id != null && id.length() > 0) {
                Connection con = ConUtil.getConn(MysqlConfig.DRIVER, MysqlConfig.URL, MysqlConfig.MYSQL_USER, MysqlConfig.MYSQL_PASSWORD);
                Statement stmt;
                ResultSet ret;
                try {
                    stmt = con.createStatement();
                    ret = stmt.executeQuery(FlinkConstant.getExecuteSql(id));
                    if (ret.next()) {
                        do {
                            flinkSqlListTemp.add(ret.getString(1));
                            String config = ret.getString(2);
                            if (config != null && config.length() != 0) {
                                envConfig = config;
                            }
                        } while (ret.next());
                    } else {
                        ConUtil.close(con, stmt, ret);
                        return envConfig;
                    }


                } catch (SQLException e1) {
                    logger.error("任务异常");
                    logger.error("执行sql失败", e1);
                    logger.error("↑↑↑↑↑↑↑↑↑  任务异常结束 end ↑↑↑↑↑↑");
                }
            }
        }

        List<String> strings = SqlInterParseHelper.getInstance(flinkSqlListTemp, configMap).parseOutSqlList();
        flinkSqlList.addAll(strings);
        return envConfig;
    }

    /**
     * @author lcg
     * @operate 获取字典函数的数据
     * @date 2021/9/17 9:13
     */
    public static void getCodeOrValueFunctionData(String dictStr) {
        Connection con = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            String mysqlDictQuery = FlinkConstant.sqlQueryMysql;
            if (!dictStr.isEmpty()) {
                mysqlDictQuery = FlinkConstant.getDictExecuteSql(dictStr);
            }
            con = ConUtil.getConn(MysqlConfig.DRIVER, MysqlConfig.URL, MysqlConfig.MYSQL_USER, MysqlConfig.MYSQL_PASSWORD);
            statement = con.createStatement();
            resultSet = statement.executeQuery(mysqlDictQuery);
            while (resultSet.next()) {
                MainApp.allDataSet.add(new org.apache.flink.api.java.tuple.Tuple6<>(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3),
                        resultSet.getString(4), resultSet.getString(5), resultSet.getString(6)));
            }
        } catch (Exception er) {
            logger.error(er.getMessage());
        } finally {
            ConUtil.close(con, statement, resultSet);
        }
    }


    /**
     * @return void
     * @author lcg
     * @operate 获取DIM数据
     * @date 2022/1/21 17:07
     */
    public static Map<String, String> getCodeByValue(String tableName, String dictValue, String codeColumn, String valueColumn, String dictColumn) {
        Map<String, String> res = new ConcurrentHashMap<>();
        Connection con = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            String mysqlDictQuery;
            if (dictValue != null && dictColumn != null && !dictValue.isEmpty() && !dictColumn.isEmpty()) {
                mysqlDictQuery = FlinkConstant.sqlQueryMysqlDimTableWithWhere;
                mysqlDictQuery = String.format(mysqlDictQuery, codeColumn, valueColumn, tableName, dictColumn, dictValue);
            } else {
                mysqlDictQuery = FlinkConstant.sqlQueryMysqlDimTable;
                mysqlDictQuery = String.format(mysqlDictQuery, codeColumn, valueColumn, tableName);
            }
            con = ConUtil.getConn(MysqlConfig.DRIVER, MysqlConfig.DIM_URL, MysqlConfig.MYSQL_USER, MysqlConfig.MYSQL_PASSWORD);
            statement = con.createStatement();
            resultSet = statement.executeQuery(mysqlDictQuery);
            while (resultSet.next()) {
                res.put(resultSet.getString(codeColumn) == null ? "" : resultSet.getString(codeColumn),
                        resultSet.getString(valueColumn) == null ? "" : resultSet.getString(valueColumn));
            }
        } catch (Exception e) {
            logger.error("获取数据失败", e);
        } finally {
            ConUtil.close(con, statement, resultSet);
        }
        return res;
    }

    /**
     *
     * @author lcg
     * @operate 获取config
     * @date 2022/8/11 13:58
     * @return java.util.HashMap<java.lang.String,java.lang.String>
     */
    public static HashMap<String, String> getExecuteSqlConfig() {
        HashMap<String, String> res = new HashMap<>();
       try {
           Connection con = ConUtil.getConn(MysqlConfig.DRIVER, MysqlConfig.URL, MysqlConfig.MYSQL_USER, MysqlConfig.MYSQL_PASSWORD);
           Statement statement = con.createStatement();
           ResultSet resultSet = statement.executeQuery(FlinkConstant.getExecuteAllSqlConfig());
           while (resultSet.next()){
               res.put(resultSet.getString("name") == null ? "" : resultSet.getString("name"),
                       resultSet.getString("connect_info") == null ? "" : resultSet.getString("connect_info"));
           }
           ConUtil.close(con, statement, resultSet);
       }catch (IOException | SQLException e){
          logger.error("获取config是出错",e);
       }
        return res;
    }
}
