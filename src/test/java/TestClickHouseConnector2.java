

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.ExecutionException;

public class TestClickHouseConnector2 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

       // tableEnvironment.executeSql("SET streaming-mode=false");


        

        tableEnvironment.executeSql("CREATE TABLE  sourceTable (" +
            "    time_stamp Timestamp," +
            "    `value` DOUBLE," +
                "quality_stamp INTEGER," +
                "tag_id INTEGER"+
            ") WITH (" +
            "    'connector' = 'clickhouse'," +
            "    'url' = 'jdbc:clickhouse://node1:8123/sensordata'," +
            "    'table-name' = 'sin_gl07'," +
            "    'username' = 'default'," +
            "    'password' = 'qwert'," +
            "    'format' = 'csv'" +
            ")");



        tableEnvironment.executeSql("CREATE TABLE  sinkTable (" +
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


        tableEnvironment.executeSql("CREATE TABLE  printSinkTable (" +
                "    time_stamp Timestamp," +
                "    `value` DOUBLE," +
                "    quality_stamp INTEGER," +
                "tag_id INTEGER"+
                ") WITH (" +
                "    'connector' = 'print'" +
                ")");





        TableResult tableResult = tableEnvironment.executeSql(
                "insert into sinkTable " +
                        "select time_stamp,`value`,quality_stamp,tag_id " +
                        "from sourceTable limit 100");


        tableResult.getJobClient().get().getJobExecutionResult().get();
    }
}