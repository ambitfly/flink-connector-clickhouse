package com.ambitfly.test;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.concurrent.ExecutionException;

public class PulsarSensordataToPrint {

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
                "    TagId INT,\n" +
                "    QualityStamp INT,\n " +
                "    `TimeStamp` STRING,\n " +
                "    `Value` DOUBLE    " +
                ") WITH (\n" +
                "     'connector' = 'pulsar',\n" +
                " 'topic' = 'persistent://my-tenant/test-namespace/_sin_gl07',\n" +
                " 'value.format' = 'json',\n" +
                        "'service-url' = 'pulsar://10.10.21.229:6650',"+
                "'admin-url' = 'http://10.10.21.229:8300',"+
                "'scan.startup.mode' = 'earliest'," +
                "'generic' = 'true'"+
                ")");




        tableEnvironment.executeSql("CREATE TABLE printSinkTable (" +
                "    TagId INT,\n" +
                "    QualityStamp INT,\n " +
                "    `TimeStamp` STRING,\n " +
                "    `Value` DOUBLE    " +
                ") WITH (" +
                "    'connector' = 'print'" +

                ")");


        TableResult tableResult = tableEnvironment.executeSql(
                "insert into printSinkTable " +
                        "select *" +
                        "from sinkTable");




        tableResult.getJobClient().get().getJobExecutionResult().get();
    }
}
