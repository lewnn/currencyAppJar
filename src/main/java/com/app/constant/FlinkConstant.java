package com.app.constant;

import com.app.config.ExcutorConfig;

import java.util.ArrayList;
import java.util.Arrays;

public abstract class FlinkConstant {
    /**
     * STATE_BACKEND类型rocksdb是否启用
     */
    public final static String STATE_BACKEND_TYPE = "rocksdbEnabled";

    /**
     * 执行模式
     */
    public final static String RUNTIME_MODE = "runtimeMode";

    /**
     * 并行度名称
     */
    public final static String PARALLELISM_NAME = "parallelism";

    /**
     * Directory for checkpoints filesystem, when using any of the default bundled
     */
    public final static String STATE_BACKEND_DIR = "hdfs:///flink12/flink-checkpoints";

    /**
     * 字典函数函数名称
     */
    public final static String UDF_TO_DICT_MAPPING = "TO_DICT_MAPPING";

    /**
     * 根据key获取value
     */
    public final static String UDF_TO_GET_VALUE_BY_KEY = "GET_VALUE";

    /**
     * 根据key获取value
     */
    public final static String UDF_TO_GET_DAY_GAP = "GET_DAY_GAP";

    /**
     * 字典函数函数名称的字典字段的index
     */
    public final static int UDF_TO_DICT_MAPPING_DICT_INDEX = 1;

    /**
     * TO_MAP 的函数名称
     */
    public final static String UDF_TO_MAP = "TO_MAP";

    /**
     * GET_KEY 的函数名称
     */
    public final static String UDF_GET_KEY = "GET_KEY";

    //自定义函数列表
    public final static ArrayList<String> UDF_LIST = new ArrayList<>(Arrays.asList(UDF_TO_DICT_MAPPING, UDF_TO_MAP, UDF_GET_KEY, UDF_TO_GET_VALUE_BY_KEY, UDF_TO_GET_DAY_GAP));

    /**
     * 根据数据里的源code获取目标code
     */
    public final static String CODE_CODE = "C2C";
    /**
     * 根据数据里的源code获取目标value
     */
    public final static String CODE_VALUE = "C2V";
    /**
     * 根据数据里的源value获取目标value
     */
    public final static String VALUE_CODE = "V2C";
    /**
     * 根据数据里的源code获取目标value
     */
    public final static String VALUE_VALUE = "V2V";

    /**
     * 创建flink 连接参数
     */
    public static final String sqlCreateTb = "CREATE TABLE DICT_MAPPING ( dict_version STRING, source_code STRING, source_value STRING, source_dict STRING, target_code STRING, target_value STRING ) WITH ( 'connector' = 'jdbc', 'url' = 'jdbc:mysql://" + ExcutorConfig.MYSQL_IP + ":3306/dfly', 'table-name' = 'dlink_dict_mapping', 'username' = '" + ExcutorConfig.MYSQL_USER + "', 'password' = '" + ExcutorConfig.MYSQL_PASSWORD + "' )";
    /**
     * 查询相关字段
     */
    public static final String sqlQuery = "SELECT dict_version,source_code,source_value,source_dict,target_code,target_value from DICT_MAPPING";
    /**
     * 查询mysql
     */
    public static final String sqlQueryMysql = "SELECT dict_version,source_code,source_value,source_dict,target_code,target_value from dlink_dict_mapping";

    /**
     * 查询mysql
     */
    public static final String sqlQueryMysqlDimTable = "SELECT %s ,%s  from %s";

    /**
     * 查询mysql with Where
     */
    public static final String sqlQueryMysqlDimTableWithWhere = "SELECT %s ,%s  from %s where %s = '%s'";

    /**
     * 创建AGG table的创建语句
     */
    public static final String CREATE_AGG_TABLE = "CREATEAGGTABLE";


    /**
     * 创建table的创建语句
     */
    public static final String CREATE_TABLE = "CREATETABLE";

    /**
     * doris的连接语句
     */
    public static final String CONNECTOR = "'CONNECTOR'='DORIS'";


    /**
     * doris的连接语句 sink配置
     */
    public static final String JSON_SINK_V1_1_1 = "sink.properties.format";

    /**
     * doris的连接语句 sink配置
     */
    public static final String JSON_SINK_LINE_V1_1_1 = "sink.properties.read_json_by_line";

    public static final String JSON_SINK_STRIP_OUT_JSON_ARR_V1_1_1 = "sink.properties.strip_outer_array";

    public static String getExecuteSql(String id) {
        return "SELECT  dfs.sql_text ,dt.env_config    FROM      dlink_flink_sql dfs LEFT JOIN dlink_trans  dt on dfs.trans_id = dt.id    WHERE     dfs.enabled = 1    AND dfs.id = " + id + "    ORDER BY       sql_index   ";
    }

    public static String getExecuteAllSqlConfig() {
        return " SELECT  name ,connect_info as value ,1 as flag from dbase_database WHERE connect_info  is not NULL  and connect_info != '' " +
                " union  SELECT  name,value ,2 as flag from dlink_variable dv  WHERE  dv.enabled  = 1 ";
    }
    public static String getEncodePasswordConfig() {
        return " SELECT  name ,password ,encode_flag from dbase_database WHERE   enabled is TRUE " ;
    }

    public static String getExecuteSqlPreparedPara() {
        return " SELECT  name ,connect_info from dbase_database WHERE connect_info  is not NULL  and connect_info != '' ";
    }

    public static String getTableSchemaSql(String sqlId) {
//        return " SELECT  dc.name,`data_type` dt, dt2.name as tn, dc.data_length  dl,dc.data_scale ds from dlink_columns dc , dlink_flink_sql dfs,dlink_trans  dt ,dlink_tables dt2  WHERE  dc.table_id=dt.table_id and dt.id = dfs.trans_id and dt2.id=dt.table_id and dfs.id=" + sqlId;
        return "SELECT  dc.name,data_type dt, dt2.name as tn, dc.data_length  dl,dc.data_scale ds, dc.is_pk pk from dlink_columns dc , dlink_flink_sql dfs,dlink_trans  dt ,dlink_tables dt2  WHERE  dc.table_id=dt.table_id and dt.id = dfs.trans_id and dt2.id=dt.table_id and dfs.id=" + sqlId;
    }

    public static String getDictExecuteSql(String dictStr) {
        return sqlQueryMysql + " where source_dict in " + dictStr + " or target_dict in " + dictStr;
    }

    public static String getTablesColumnsInfo(String dbName, String tableName) {
        return "SELECT    COLUMN_NAME AS 'col',DATA_TYPE AS 'type', " +
                "    CHARACTER_MAXIMUM_LENGTH AS 'varlen',NUMERIC_PRECISION AS 'numlen', " +
                "    NUMERIC_SCALE AS 'scale' " +
                "FROM  " +
                "    information_schema.`COLUMNS` " +
                "WHERE " +
                "    TABLE_SCHEMA =  '" + dbName + "'" +
                "    and TABLE_NAME= '" + tableName + "'" +
                "    ORDER by ORDINAL_POSITION";
    }

    /**
     * 左括号
     */
    public static final String BRACKET_LEFT = "(";
    /**
     * 右括号
     */
    public static final String BRACKET_RIGHT = ")";
    /**
     * 逗号
     */
    public static final String COMMA = ",";

    /**
     * isnert语句的其实判断
     */
    public static final String INSET_SQL = "INSERTINTO";


    /**
     * isnert语句的其实判断
     */
    public static final String CDC_SQL = "CDC";

    /**
     * 更新字段名称 key
     */
    public static final String UPDATE_TIME_COL = "cus.sink.update.col.";

}
