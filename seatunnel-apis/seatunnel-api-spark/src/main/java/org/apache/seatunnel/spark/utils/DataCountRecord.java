package org.apache.seatunnel.spark.utils;

import org.apache.seatunnel.spark.BaseSparkTransform;
import org.apache.seatunnel.spark.SparkEnvironment;
import org.apache.seatunnel.spark.batch.SparkBatchSink;
import org.apache.seatunnel.spark.batch.SparkBatchSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DataCountRecord {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataCountRecord.class);

    public static long sourceDataCount(Dataset<Row> source) {
        return source.count();
    }

    public static long transformDataCount(Dataset<Row> tranform) {
        return tranform.count();
    }

    public static long sinkDataCount(Dataset<Row> sink) {
        return sink.count();
    }

    public static void dataCount(SparkEnvironment environment, List<SparkBatchSource> sources, List<BaseSparkTransform> transforms, List<SparkBatchSink> sinks) throws SQLException, IOException, ClassNotFoundException {

        ArrayList<String> sourceArrayList = new ArrayList<>();
        ArrayList<String> transArrayList = new ArrayList<>();
        ArrayList<String> sinkArrayList = new ArrayList<>();
        ArrayList<String> result = new ArrayList<>();

        Config config = environment.getConfig();
        String appName = config.getString("spark.app.name");
        String propEnv;
        String isRecord;
        if (config.hasPath("spark.yarn.appMasterEnv.prop_env")) {
            propEnv = config.getString("spark.yarn.appMasterEnv.prop_env");
        }
        else {
            propEnv = "dev";
        }
        if (config.hasPath("spark.yarn.appMasterEnv.if_record")) {
            isRecord = config.getString("spark.yarn.appMasterEnv.if_record");
        }
        else {
            isRecord = "false";
        }
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
                    sourceArrayList.add(tableName + ":" + sourceDataCountStr);

                }
                for (BaseSparkTransform transform : transforms) {
                    Dataset<Row> rowDataset = SparkEnvironment.transformProcess(environment, transform, ds);
                    long transformDataCount = transformDataCount(rowDataset);
                    String transformDataCountStr = String.valueOf(transformDataCount);
                    transArrayList.add(transformDataCountStr);
                }
                for (SparkBatchSink sink : sinks) {
                    Config sinkConfig = sink.getConfig();
                    if (config.hasPath("source_table_name")) {
                        String sourceTableName = sinkConfig.getString("source_table_name");
                        Dataset<Row> sinkDataset = environment.getSparkSession().read().table(sourceTableName);
                        long sinkDataCount = sinkDataset.count();
                        String sinkDataCountStr = String.valueOf(sinkDataCount);
                        sinkArrayList.add(sinkDataCountStr);
                    }
                    else {
                        long sinkDataCount = ds.count();
                        String sinkDataCountStr = String.valueOf(sinkDataCount);
                        sinkArrayList.add(sinkDataCountStr);
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
            String appId = GetYarnInFor.getYarnInForDetail(propEnv, appName);
            LOGGER.info("appId:" + appId);
            String sqlStr = String.format("update \n" +
                    "dis_job_detailed_information \n" +
                    "set \n" +
                    "data_count='%s', application_id='%s' \n" +
                    "where \n" +
                    "job_name = '%s' \n" +
                    "and job_submit_time in (\n" +
                    "select \n" +
                    "a.atime \n" +
                    "from \n" +
                    "(select \n" +
                    "max(job_submit_time) as atime \n" +
                    "from dis_job_detailed_information \n" +
                    "where \n" +
                    "job_name = '%s') as a)", result, appId, appName, appName);
            GetConnectMysql.saveToMysql(sqlStr, propEnv);
        }
        else {
            LOGGER.info("Data volume is not recorded");
        }
    }
}
