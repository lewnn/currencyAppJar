package com.app.utils;


import com.app.config.MysqlConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;

/**
 * @author lcg
 * @version 1.0
 * @title
 * @description
 * @createDate 2020/10/26
 */
public class ConUtil {
    private static Logger logger = LoggerFactory.getLogger(ConUtil.class);
    public static Connection conn = null;

    /**
     * @return java.sql.Connection
     * @author lcg
     * @operate 连接mysql
     * @date 2021/9/16 9:57
     */
    public static Connection getConn(String driver, String url, String username, String password) throws IOException {
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
        } catch (SQLException | ClassNotFoundException e) {
            logger.error("获取数据库连接失败", e);
            close();
        }
        return conn;
    }



    public static void close() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            logger.error("关闭Connect失败", e);
        }
    }

    public static void close(Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            logger.error("关闭Statement失败", e);
        }
    }

    public static void close(ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException e) {
            logger.error("关闭ResultSet失败", e);
        }
    }
    public static void close(Connection conn, Statement statement, ResultSet resultSet) {
       close(resultSet);
       close(statement);
       close();
    }

}
