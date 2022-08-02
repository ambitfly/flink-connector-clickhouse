package com.ambitfly.test;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

public class PulsarSensordataToClickHouse {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                //.inBatchMode()
                .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        // tableEnvironment.executeSql("SET streaming-mode=false");
        tableEnvironment.executeSql("\n" +
                "CREATE TABLE sinkTable (\n" +
                "    table_name STRING,\n" +
                "    time_stamp STRING,\n" +
                "    `value` DOUBLE,\n" +
                "    quality_stamp INT,\n " +
                "    `tag_id` INT\n " +
                ") WITH (\n" +
                "     'connector' = 'pulsar',\n" +
                " 'topic' = 'persistent://my-tenant/test-namespace/_sin_gl07_sql_csv',\n" +
                " 'value.format' = 'csv',\n" +
                        "'service-url' = 'pulsar://10.10.21.229:6650',"+
                "'admin-url' = 'http://10.10.21.229:8300',"+
                "'scan.startup.mode' = 'earliest'," +
                "'generic' = 'true'"+
                ")");




        tableEnvironment.executeSql("CREATE TABLE  sourceTable (" +
                "    time_stamp Timestamp," +
                "    `value` DOUBLE," +
                "    quality_stamp INTEGER," +
                "tag_id INTEGER"+
                ") WITH (" +
                "    'connector' = 'clickhouse'," +
                "    'url' = 'jdbc:clickhouse://node1:8123/sensordata'," +
                "    'table-name' = 'sin_gl09'," +
                "    'username' = 'default'," +
                "    'password' = 'qwert'," +
                "    'format' = 'csv'" +
                ")");



        TableResult tableResult = tableEnvironment.executeSql(
                "insert into sourceTable" +
                        "select `time_stamp`,`value`,quality_stamp,tag_id" +
                        "from sinkTable");




        tableResult.getJobClient().get().getJobExecutionResult().get();
    }
}
