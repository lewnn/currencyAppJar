package com.app;

import com.app.cdc.BaseCdc;
import com.app.check.FlinkSqlCheck;
import com.app.constant.FlinkConstant;
import com.app.entity.AggTablePara;
import com.app.entity.DataTypeProcess;
import com.app.executor.CdcExecutor;
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
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lcg
 * @version 1.0
 * @Date 2020/10/26
 */
public class MainApp {
    private static final Logger logger = LoggerFactory.getLogger(MainApp.class);

    public static volatile HashSet<Tuple6<String, String, String, String, String, String>> allDataSet = new HashSet<>();

    public static ConcurrentHashMap<String, List<DataTypeProcess>> dataSchema = new ConcurrentHashMap<>();

    // 主方法 入口
    public static void main(String[] args) throws Exception {
        logger.info("任务开始");
        ParameterTool parameters = ParameterTool.fromArgs(args);
        String idParas = parameters.get("id", null);
        logger.info("idParas:" + idParas);
        String envConfig;
        if (idParas != null && !"".equals(idParas)) {
            List<String> flinkSqlList = new ArrayList<>();
            String[] ids = idParas.split(",");
            envConfig = ExecuteSqlProcess.getExecuteSql(ids, flinkSqlList);
            boolean sqlMultiInsert = FlinkSqlCheck.getSqlMultiInsertMode(flinkSqlList);
            boolean cdcMode = FlinkSqlCheck.getSqlCdcMode(flinkSqlList);
            if (flinkSqlList.isEmpty()) {
            } else if (sqlMultiInsert) {
                List<String> sourceSql = new ArrayList<>();
                List<String> sinkSql = new ArrayList<>();
                FlinkSqlCheck.getSqlMultiInsertAndCreate(flinkSqlList, sourceSql, sinkSql);
                executeSql(sourceSql, sinkSql, envConfig);
                logger.info("合并模式任务提交结束");
            } else if (cdcMode) {
                BaseCdc baseCdc = BaseCdc.getInstance(flinkSqlList.get(0), ids[0].split(",")[0]);
                baseCdc.loadTableSchema();//加载表结构
                ConcurrentHashMap<String, OutputTag<Map>> outputTagMap = new ConcurrentHashMap<>();
                for (String tag : baseCdc.getTables().split(",")) {
                    outputTagMap.put(tag, new OutputTag<Map>(tag) {
                    });
                }
               new CdcExecutor(outputTagMap).executeSql(baseCdc);
            } else {
                executeSql(flinkSqlList, new ArrayList<>(), envConfig);
                logger.info("任务结束");
            }
        }
    }

    /**
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
        if (runTimeMode.equals(RuntimeExecutionMode.BATCH)) {
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
            if (!sinkSql.isEmpty()) {
                for (String sink : sinkSql) {
                    statementSet.addInsertSql(sink);
                }
                statementSet.execute();
            }
        } catch (Exception e) {
            e.printStackTrace();
//            logger.error("任务异常", e);
        }
    }


}







