package com.ambitfly.test;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.TableViewBuilder;

import java.util.concurrent.ExecutionException;

public class PulsarToPrint {

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
                "    name STRING,\n" +
                "    age INT,\n" +
                "    sex STRING\n" +
                ") WITH (\n" +
                "     'connector' = 'pulsar',\n" +
                " 'topic' = 'persistent://public/default/test_student',\n" +
                " 'value.format' = 'csv',\n" +
                        "'service-url' = 'pulsar://10.10.21.229:6650',"+
                "'admin-url' = 'http://10.10.21.229:8300',"+
                "'scan.startup.mode' = 'earliest'," +
                "'generic' = 'true'"+
                ")");




        tableEnvironment.executeSql("CREATE TABLE printSinkTable (" +
                "    name STRING," +
                "    age INTEGER," +
                "    sex STRING" +
                ") WITH (" +
                "    'connector' = 'print'" +

                ")");


        TableResult tableResult = tableEnvironment.executeSql(
                "insert into printSinkTable " +
                        "select name,age,sex " +
                        "from sinkTable");




        tableResult.getJobClient().get().getJobExecutionResult().get();
    }
}
