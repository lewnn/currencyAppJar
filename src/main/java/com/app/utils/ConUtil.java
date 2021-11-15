package com.app.utils;


import com.app.config.MysqlConfig;

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
    public static Connection conn = null;

    /**
     * @return java.sql.Connection
     * @author lcg
     * @operate 连接mysql
     * @date 2021/9/16 9:57
     */
    public static Connection getConn() throws IOException {
        String driver = MysqlConfig.DRIVER;
        String url = MysqlConfig.URL;
        String username = MysqlConfig.MYSQL_USER;
        String password = MysqlConfig.MYSQL_PASSWORD;
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
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
            e.printStackTrace();
        }
    }

    public static void close(Statement statement) {
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void close(ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public static void close(Connection conn, Statement statement, ResultSet resultSet) {
       close(resultSet);
       close(statement);
       close();
    }

}
