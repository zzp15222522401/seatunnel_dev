package org.apache.seatunnel.spark.utils;

import org.apache.seatunnel.spark.BaseSparkTransform;
import org.apache.seatunnel.spark.SparkEnvironment;
import org.apache.seatunnel.spark.batch.SparkBatchSink;
import org.apache.seatunnel.spark.batch.SparkBatchSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class DataCountRecord {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataCountRecord.class);

    private static long sourceDataCount(Dataset<Row> source) {
        return source.count();
    }

    private static long transformDataCount(Dataset<Row> tranform) {
        return tranform.count();
    }

    private static long sinkDataCount(Dataset<Row> sink) {
        return sink.count();
    }

    private static final long ZERO = 0;

    private static String getPropEnv(Config config) {
        String propEnv;
        if (config.hasPath("spark.yarn.appMasterEnv.prop_env")) {
            propEnv = config.getString("spark.yarn.appMasterEnv.prop_env");
        }
        else {
            propEnv = "dev";
        }
        return propEnv;
    }

    private static String getIfRecord(Config config) {
        String isRecord;
        if (config.hasPath("spark.yarn.appMasterEnv.if_record")) {
            isRecord = config.getString("spark.yarn.appMasterEnv.if_record");
        }
        else {
            isRecord = "false";
        }
        return isRecord;
    }

    private static List<String> getFeiShuUrl(Config config) {
        ArrayList<String> urlLists = new ArrayList<>();
        if (config.hasPath("spark.yarn.appMasterEnv.fs_url")) {
            String urls = config.getString("spark.yarn.appMasterEnv.fs_url");
            String[] split = urls.split(";");
            urlLists.addAll(Arrays.asList(split));
        }
        else {
            urlLists.add("https://open.feishu.cn/open-apis/bot/v2/hook/262624b4-17b5-472a-9cea-8e40296b4173");
        }
        return urlLists;
    }

    public static void dataCount(SparkEnvironment environment, List<SparkBatchSource> sources, List<BaseSparkTransform> transforms, List<SparkBatchSink> sinks) throws SQLException, IOException, ClassNotFoundException {

        ArrayList<String> sourceArrayList = new ArrayList<>();
        ArrayList<String> transArrayList = new ArrayList<>();
        ArrayList<String> sinkArrayList = new ArrayList<>();
        ArrayList<String> result = new ArrayList<>();
        ArrayList<String> argsList = new ArrayList<>();
        SparkConf sparkConf = new SparkConf();
        String joinStr = "";
        if (!sparkConf.get("spark.executor.extraJavaOptions", "").isEmpty()) {
            String tmpArgs = sparkConf.get("spark.executor.extraJavaOptions");
            String[] split = tmpArgs.split(" ");
            for (String s : split) {
                String s1 = s.split("=")[1];
                argsList.add(s1);
            }
            joinStr = String.join("-", argsList);
        }
        LOGGER.info(joinStr);
        Config config = environment.getConfig();
        String appName = config.getString("spark.app.name");
        String isRecord = DataCountRecord.getIfRecord(config);
        String propEnv = DataCountRecord.getPropEnv(config);
        if (!"false".equals(isRecord)) {
            String tableName = "Source";
            if (!sources.isEmpty()) {
                Dataset<Row> ds = sources.get(0).getData(environment);
                for (SparkBatchSource source : sources) {
                    long sourceDataCount = sourceDataCount(source.getData(environment));
                    String sourceDataCountStr = String.valueOf(sourceDataCount);
                    Config sourceConfig = source.getConfig();
                    if (sourceConfig.hasPath("result_table_name")) {
                        tableName = sourceConfig.getString("result_table_name");
                    }
                    sourceArrayList.add(tableName + "-source:" + sourceDataCountStr);
                    String name;
                    if (!"".equals(joinStr)) {
                        name = String.format("Job:%s \nTable:%s \nArgs:%s", appName, tableName, joinStr);
                    }
                    else {
                        name = String.format("Job:%s \nTable:%s", appName, tableName);
                    }
                    List<String> feiShuUrl = getFeiShuUrl(config);
                    for (String url : feiShuUrl) {
                        if (ZERO == sourceDataCount) {
                            FeiShuWarning.sendFeiShuWarningInfo(name, url);
                        }
                    }
                }
                for (BaseSparkTransform transform : transforms) {
                    Dataset<Row> rowDataset = SparkEnvironment.transformProcess(environment, transform, ds);
                    long transformDataCount = transformDataCount(rowDataset);
                    String transformDataCountStr = String.valueOf(transformDataCount);
                    transArrayList.add("transform:" + transformDataCountStr);
                }
                for (SparkBatchSink sink : sinks) {
                    Config sinkConfig = sink.getConfig();
                    if (sinkConfig.hasPath("source_table_name")) {
                        String sourceTableName = sinkConfig.getString("source_table_name");
                        Dataset<Row> sinkDataset = environment.getSparkSession().read().table(sourceTableName);
                        long sinkDataCount = sinkDataset.count();
                        String sinkDataCountStr = String.valueOf(sinkDataCount);
                        sinkArrayList.add("sink:" + sinkDataCountStr);
                    }
                    else {
                        long sinkDataCount = ds.count();
                        String sinkDataCountStr = String.valueOf(sinkDataCount);
                        sinkArrayList.add("sink:" + sinkDataCountStr);
                    }
                }
            }
            for (String s : sourceArrayList) {
                int index = sourceArrayList.indexOf(s);
                String trans1 = transArrayList.get(index);
                String sink1 = sinkArrayList.get(index);
                String tmpStr = String.format("%s,%s,%s ", s, trans1, sink1);
                result.add(tmpStr);
            }
            String resultCount = String.join(",", result);
            LOGGER.info("resultStr:" + resultCount);
            String appId = GetYarnInFor.getYarnInForDetail(propEnv, appName);
            LOGGER.info("appId:" + appId);
            Date date = new Date();
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            String now = dateFormat.format(date);
            String insertSql = String.format("insert into dis_job_detailed_information (job_name,fieldmapping,job_submit_time,dag_id,pre_task_id,task_id,\n" +
                                             "create_time,sqlfilename,propfilename, source_name, target_name,\n" +
                                             "application_id,data_count,job_status,final_status)" +
                                             "select a.job_name,fieldmapping,'%s',dag_id,pre_task_id,task_id,'%s', \n" +
                                             "sqlfilename, propfilename, a.source_name, a.target_name, '%s','%s','%s','%s' \n" +
                                             "from dis_job_detailed_information a left join dis_instance_record b on a.job_name = b.job_name \n" +
                                             "and a.source_name =b.source_name and a.target_name =b.target_name where a.job_name = '%s' and b.job_name ='%s'\n" +
                                             "order by a.create_time desc limit 1",
                                             now, now, appId, resultCount, "FINISHED", "SUCCEEDED", appName,  appName);
            GetConnectMysql.saveToMysql(insertSql, propEnv);
            LOGGER.info(String.format("Data volume is recorded-%s", resultCount));
        }
        else {
            LOGGER.info("Data volume is not recorded");
        }
    }
}
