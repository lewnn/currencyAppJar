package com.app.utils;


import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class SqlInterParseHelper {

    public final static String DEFINED_PARA = ":=";
    private final List<String> inSqlList; //传入的sql集合
    private final List<String> outList = new ArrayList<>();  //传出的sql集合
    private List<String> paraSqlList; // 参数sql
    private Map<String, String> paraMap = new HashMap<>(); //参数map

    /**
     * @return java.util.List<java.lang.String>
     * @author lcg
     * @operate 获取处理后sql List  多条
     * @date 2022/8/9 9:15
     */
    public List<String> parseOutSqlList() {
        for (String sqlText : inSqlList) {
            String[] split = sqlText.split(";");
            List<String> sqlRes = new ArrayList<>();
            for (String sql : split) {
                if (!sql.contains(DEFINED_PARA) && !sql.trim().isEmpty()) {
                    sqlRes.add(parseOutOneSql(sql));
                }
            }
            if (!sqlRes.isEmpty()) {
                outList.add(StringUtils.join(sqlRes, ";"));
            }
        }
        return outList;
    }

    //处理单条sql
    public String parseOutOneSql(String sql) {
        return parseTemplate(sql, this.paraMap).replaceAll("[\t\n\r]", "");
    }

    /**
     * @author lcg
     * @operate 将参数sql，转换为参数Map
     * @date 2022/8/9 9:15
     */
    public void parseParaMap() {
        this.paraSqlList = inSqlList.stream()
                .filter(fSql -> fSql.replace(" ", "").contains(DEFINED_PARA))
                .flatMap(fSql -> Arrays.stream(fSql.replace(" ", "").split(";")).filter(sql -> sql.contains(DEFINED_PARA)))
                .peek(sql -> {
                    String[] split = sql.split(DEFINED_PARA);
                    paraMap.put(split[0].replaceAll("[\t\n\r\\s+]", ""), split[1]);
                })
                .collect(Collectors.toList());
    }

    private SqlInterParseHelper(List<String> inSqlList) {
        this.inSqlList = inSqlList;
        parseParaMap();
    }

    private SqlInterParseHelper(List<String> inSqlList, Map<String, String> paraMap) {
        this.inSqlList = inSqlList;
        this.paraMap = paraMap == null ? new HashMap<>():paraMap;
        parseParaMap();
    }

    private SqlInterParseHelper(Map<String, String> paraMap) {
        this.inSqlList = new ArrayList<>();
        this.paraMap = paraMap == null ? new HashMap<>():paraMap;
        parseParaMap();
    }


    public static SqlInterParseHelper getInstance(List<String> inSqlList) {
        return new SqlInterParseHelper(inSqlList);
    }

    public static SqlInterParseHelper getInstance(List<String> inSqlList, Map<String, String> paraMap) {
        return new SqlInterParseHelper(inSqlList, paraMap);
    }

    public static SqlInterParseHelper getInstance(Map<String, String> paraMap) {
        return new SqlInterParseHelper(paraMap);
    }

    public static String parseTemplate(String template, Map<String, String> properties) {
        if (template == null || template.isEmpty() || properties == null) {
            return template;
        }
        String reg = "\\$\\{([^\\}]+)\\}";
        Pattern pattern = Pattern.compile(reg);
        Matcher matcher = pattern.matcher(template);
        while (matcher.find()) {
            String group = matcher.group();
            Object o = properties.get(group.replaceAll(reg, "$1").replace("\\s+", ""));
            if (o != null) {
                template = template.replace(group, String.valueOf(o));
            } else {
                template = template.replace(group, "");
            }
        }
        return template;
    }


}
