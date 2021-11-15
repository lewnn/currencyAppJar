package com.app.utils;

import com.app.MainApp;
import com.app.constant.FlinkConstant;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.List;

/**
 * @author lcg
 * @operate 获取执行的sql
 * @date 2021/9/16 9:46
 * @return
 */
public class ExecuteSqlProcess {

    /**
     * @return java.lang.String
     * @author lcg
     * @operate 获取sql
     * @date 2021/9/16 9:54
     */
    public static String getExecuteSql(String[] ids, List<String> flinkSqlList) throws IOException {
        String envConfig = "";
        for (int i = 0; i < ids.length; i++) {
            if (ids[i] != null && ids[i].length() > 0) {
                Connection con = ConUtil.getConn();
                Statement stmt = null;
                ResultSet ret = null;
                try {
                    stmt = con.createStatement();
                    ret = stmt.executeQuery(FlinkConstant.getExecuteSql(ids[i]));
                    if (ret.next()) {
                        do {
                            flinkSqlList.add(ret.getString(1));
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
                    e1.printStackTrace();
                    String message = e1.getMessage();
                    System.err.println(LocalDateTime.now().toString() + "任务异常");
                    System.err.println(message.length());
                    System.out.println("↑↑↑↑↑↑↑↑↑  任务异常结束 end ↑↑↑↑↑↑");
                }
            }
        }
        return envConfig;
    }

    /**
     *
     * @author lcg
     * @operate 获取字典函数的数据
     * @date 2021/9/17 9:13
     * @return void
     */
    public static void getCodeOrValueFunctionData(String dictStr) {
        Connection con = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            String mysqlDictQuery = FlinkConstant.sqlQueryMysql;
            if(!dictStr.isEmpty()){
                mysqlDictQuery = FlinkConstant.getDictExecuteSql(dictStr);
            }
            con = ConUtil.getConn();
            statement = con.createStatement();
            resultSet = statement.executeQuery(mysqlDictQuery);
            while (resultSet.next()) {
                MainApp.allDataSet.add(new org.apache.flink.api.java.tuple.Tuple6<>(resultSet.getString(1), resultSet.getString(2), resultSet.getString(3),
                        resultSet.getString(4), resultSet.getString(5), resultSet.getString(6)));
            }
        } catch (Exception er) {
            System.err.println(er.getMessage());
        } finally {
            ConUtil.close(con, statement, resultSet);
        }
    }

}
