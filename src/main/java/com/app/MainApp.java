package com.app;

import com.app.check.FlinkSqlCheck;
import com.app.constant.FlinkConstant;
import com.app.entity.AggTablePara;
import com.app.utils.ExecuteSqlProcess;
import com.app.utils.FlinkUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.*;

/**
 * @author lcg
 * @version 1.0
 * @title
 * @description
 * @createDate 2020/10/26
 */
public class MainApp {
    private static Logger logger = LoggerFactory.getLogger(MainApp.class);

    public static volatile HashSet<Tuple6<String, String, String, String, String, String>> allDataSet = new HashSet<>();

    // 主方法 入口
    public static void main(String[] args) throws IOException {
        logger.info( "任务开始");
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String idParas = parameters.get("id", null);
        String envConfig = "";
        if (idParas == null || "".equals(idParas)) {
            return;
        } else {
            List<String> flinkSqlList = new ArrayList<>();
            String[] ids = idParas.split(",");
            envConfig = ExecuteSqlProcess.getExecuteSql(ids, flinkSqlList);
            boolean sqlMultiInsert = FlinkSqlCheck.getSqlMultiInsertMode(flinkSqlList);
            if (flinkSqlList.isEmpty()) {
                return;
            } else if (sqlMultiInsert) {
                List<String> sourceSql = new ArrayList<>();
                List<String> sinkSql = new ArrayList<>();
                FlinkSqlCheck.getSqlMultiInsertAndCreate(flinkSqlList, sourceSql, sinkSql);
                executeSql(sourceSql, sinkSql, envConfig);
                logger.info( "合并模式任务提交结束");
            } else {
                executeSql(flinkSqlList, new ArrayList<>(), envConfig);
                logger.info( "任务结束");
            }
        }
    }

    /**
     * @return void
     * @author lcg
     * @operate 执行sql 及 注册函数
     * @date 2021/9/16 10:16
     */
    public static void executeSql(List<String> listSql, List<String> sinkSql, String envConfig) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, Object> mapFromJsonStr = FlinkUtils.getMapFromJsonStr(envConfig);
        FlinkUtils.configEnvPara(mapFromJsonStr, streamEnv);
        TableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);

        logger.info(" 参数 " + mapFromJsonStr);
        RuntimeExecutionMode runTimeMode = FlinkUtils.getRunTimeMode(mapFromJsonStr);
        if(runTimeMode.equals(RuntimeExecutionMode.BATCH)){
            TableConfig preConfig = streamTableEnv.getConfig();
            logger.info("执行模式已切换至【TableEnvironment】【BatchMode】");
            Configuration configuration = preConfig.getConfiguration();
            configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
            FlinkUtils.configConfiguration(mapFromJsonStr, configuration);
            streamTableEnv = TableEnvironment.create(configuration);
            logger.info(configuration.toMap().toString());
        }
        try {
            StatementSet statementSet = streamTableEnv.createStatementSet();
            if (listSql != null && listSql.size() > 0) {
                //检验是否有自定义函数
                FlinkUtils.registerFunctionOfFlink(streamTableEnv, listSql);
                for (String sqlText : listSql) {
                    if (sqlText.replace(" ", "").toUpperCase().contains(FlinkConstant.CREATE_AGG_TABLE)) {
                        AggTablePara aggTablePara = AggTablePara.buildAggTable(sqlText);
                        AggTablePara.createAggTable(streamTableEnv, aggTablePara);
                    } else {
                        streamTableEnv.executeSql(sqlText);
                    }
                }
            } else {
                return;
            }
            if(!sinkSql.isEmpty()){
                for (String sink : sinkSql) {
                    statementSet.addInsertSql(sink);
                }
                statementSet.execute();
            }
        } catch (Exception e) {
            logger.error("任务异常",e);
        }
    }


}







