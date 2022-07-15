

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class TestClickHouseConnector {
    public static void main(String[] args) throws ExecutionException, InterruptedException {


        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                //.inBatchMode()
                .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

       // tableEnvironment.executeSql("SET streaming-mode=false");
        tableEnvironment.executeSql("\n" +
                "CREATE TABLE user_mysql (\n" +
                "    id INT,\n" +
                "    name STRING,\n" +
                "    age INT\n" +
                ") WITH (\n" +
                "     'connector' = 'jdbc',\n" +
                " 'url' = 'jdbc:mysql://node2:3306/test?useSSL=false',\n" +
                " 'table-name' = 'user',\n" +
                " 'username' = 'root',\n" +
                        "  'password' = '@BJcenter123'" +
                ")");




        tableEnvironment.executeSql("CREATE TABLE sinkTable (" +
            "    id INTEGER," +
            "    name String," +
                "    age INTEGER" +
            ") WITH (" +
            "    'connector' = 'clickhouse'," +
            "    'url' = 'jdbc:clickhouse://node1:8123/sensordata'," +
            "    'table-name' = 'user'," +
            "    'username' = 'default'," +
            "    'password' = 'qwert'," +
            "    'format' = 'csv'" +
            ")");


        tableEnvironment.executeSql("CREATE TABLE printSinkTable (" +
                "    id INTEGER," +
                "    name String," +
                "    age INTEGER" +
                ") WITH (" +
                "    'connector' = 'print'" +

                ")");


        TableResult tableResult = tableEnvironment.executeSql(
                "insert into printSinkTable " +
                        "select id,name,age " +
                        "from sinkTable");


        tableResult.getJobClient().get().getJobExecutionResult().get();
    }
}