package com.app.entity;

import com.app.utils.GetSubStr;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;

/**
 * 聚合实体
 */
public class AggTablePara {
    String name; //聚合后的表名
    String columns;//聚合的列名
    String table;//聚合数据的来源表名
    List<String> filter;//过滤条件
    String groupBy;//分组条件
    String aggBy;//聚合条件

    public static AggTablePara buildAggTable(String sqlIn) {
        //去除 换行  转大写
        String orginal = sqlIn.replaceAll("(\\\r\\\n|\\\r|\\\n|\\\n\\\r)", " ");
        String sqlTemp = sqlIn.replaceAll("(\\\r\\\n|\\\r|\\\n|\\\n\\\r)", " ").toUpperCase();
        String name = GetSubStr.getSubStringPara(orginal, sqlTemp, "AGGTABLE", "AGGTABLE", "AS", "", false);
        String columns = GetSubStr.getSubStringPara(orginal, sqlTemp, "SELECT", "SELECT", "FROM", "", false);
        String table = GetSubStr.getSubStringPara(orginal, sqlTemp, "FROM", "FROM", "WHERE", "GROUP", false);
        String groupBy = GetSubStr.getSubStringPara(orginal, sqlTemp, "GROUP", "BY", "AGG", "", false);
        String aggBy = GetSubStr.getSubStringPara(orginal, sqlTemp, "AGG", "BY", "", "", false);
        List<String> filter = GetSubStr.getSubStringList(orginal, sqlTemp, "WHERE", "GROUP", "AND");
        return new AggTablePara(name, columns, table, filter, groupBy, aggBy);
    }

    private AggTablePara() {

    }

    public AggTablePara(String name, String columns, String table, List<String> filter, String groupBy, String aggBy) {
        this.name = name;
        this.columns = columns;
        this.table = table;
        this.filter = filter;
        this.groupBy = groupBy;
        this.aggBy = aggBy;
    }

    /**
     * 创建聚合表
     * @param streamTableEnv
     * @param aggTablePara
     */
    public static void createAggTable(StreamTableEnvironment streamTableEnv, AggTablePara aggTablePara) {
        Table tab = streamTableEnv.sqlQuery("select * from "+ aggTablePara.getTable());
        List<String> filter = aggTablePara.getFilter();
        for (String s : filter) {
            tab = tab.filter(s);
        }
        Table sid = tab.groupBy(aggTablePara.getGroupBy())
                .flatAggregate(aggTablePara.getAggBy())
                .select(aggTablePara.getColumns());
        streamTableEnv.registerTable(aggTablePara.getName(), sid);
    }


    public String getName() {
        return name;
    }

    public String getColumns() {
        return columns;
    }

    public String getTable() {
        return table;
    }

    public List<String> getFilter() {
        return filter;
    }

    public String getGroupBy() {
        return groupBy;
    }

    public String getAggBy() {
        return aggBy;
    }


}
