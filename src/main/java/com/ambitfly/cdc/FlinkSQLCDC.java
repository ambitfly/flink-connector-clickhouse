package com.ambitfly.cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQLCDC {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.创建 Flink-MySQL-CDC 的 Source
        tableEnv.executeSql("CREATE TABLE user_info (" +
                            " id INTEGER  primary key," +
                            " name STRING," +
                            " age INTEGER" +
                            ") WITH (" +
                            " 'connector' = 'mysql-cdc'," +
                            " 'scan.startup.mode' = 'latest-offset'," +
                            " 'hostname' = 'node2'," +
                            " 'port' = '3306'," +
                            " 'username' = 'root'," +
                            " 'password' = '@BJcenter123'," +
                            " 'database-name' = 'test'," +
                            " 'table-name' = 'user'" +
                            ")");
        //3. 查询数据并转换为流输出
        Table table = tableEnv.sqlQuery("select * from user_info");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();
        //4. 启动
        env.execute("FlinkSQLCDC");
    }
}
