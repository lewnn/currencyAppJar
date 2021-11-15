package com.app.check;

import com.app.constant.FlinkConstant;

import java.util.List;

public class FlinkSqlCheck {

    public static boolean getSqlMultiInsertMode(List<String> sqlList) {
        return sqlList.stream().filter(x -> x.toUpperCase().replace(" ", "").indexOf(FlinkConstant.INSET_SQL) > -1).count() > 1;
    }

    /**
     * @return boolean
     * @author lcg
     * @operate 获取INSERT和create的语句
     * @date 2021/11/15 13:25
     */
    public static void getSqlMultiInsertAndCreate(List<String> sqlList, List<String> sourceSql, List<String> sinkSql) {
        if (getSqlMultiInsertMode(sqlList)) {
            for (String sql : sqlList) {
                if (sql.replace(" ", "").indexOf(FlinkConstant.INSET_SQL) > -1) {
                    sinkSql.add(sql);
                } else {
                    sourceSql.add(sql);
                }
            }
        }
    }
}
