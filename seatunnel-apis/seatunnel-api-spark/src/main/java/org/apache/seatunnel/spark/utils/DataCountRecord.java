package org.apache.seatunnel.spark.utils;

import org.apache.seatunnel.spark.BaseSparkTransform;
import org.apache.seatunnel.spark.SparkEnvironment;
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

    public static void dataCount(SparkEnvironment environment, List<SparkBatchSource> sources, List<BaseSparkTransform> transforms, Dataset<Row> ds) throws SQLException, IOException, ClassNotFoundException {

        ArrayList<String> sourceArrayList = new ArrayList<>();
        ArrayList<String> transArrayList = new ArrayList<>();
        ArrayList<String> sinkArrayList = new ArrayList<>();
        ArrayList<String> result = new ArrayList<>();

        Config config = environment.getConfig();
        String appName = config.getString("spark.app.name");
        String propEnv = System.getenv("prop_env");
        if (propEnv == null) {
            propEnv = "dev";
        }
        String isRecord = System.getenv("data_record");
        if (isRecord == null){
            isRecord = "false";
        }
        if (!"false".equals(isRecord)) {
            String tableName = "Source";
            if (!sources.isEmpty()) {
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
                long sinkDataCount = ds.count();
                String sinkDataCountStr = String.valueOf(sinkDataCount);
                sinkArrayList.add(sinkDataCountStr);
            }
            for (String s : sourceArrayList) {
                int index = sourceArrayList.indexOf(s);
                String trans1 = transArrayList.get(index);
                String sink1 = sinkArrayList.get(index);
                String tmpStr = String.format("%s,%s,%s ", s, trans1, sink1);
                result.add(tmpStr);
            }
            String sqlStr = String.format("update \n" +
                    "dmp_test.dis_job_detailed_information \n" +
                    "set \n" +
                    "data_count='%s' \n" +
                    "where \n" +
                    "job_name = '%s' \n" +
                    "and job_submit_time in (\n" +
                    "select \n" +
                    "a.atime \n" +
                    "from \n" +
                    "(select \n" +
                    "max(job_submit_time) as atime \n" +
                    "from dmp_test.dis_job_detailed_information \n" +
                    "where \n" +
                    "job_name = '%s') as a)", result, appName, appName);
            GetConnectMysql.saveToMysql(sqlStr, propEnv);
        }
        else {
            LOGGER.info("Data volume is not recorded");
        }
    }
}
