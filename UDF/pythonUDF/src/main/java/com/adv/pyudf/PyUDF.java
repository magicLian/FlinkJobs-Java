package com.adv.pyudf;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class PyUDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        TableResult show_tables = tEnv.executeSql("show tables");
        show_tables.print();

        TableResult tableResult = tEnv.executeSql("CREATE TABLE device_temperature (\n" +
                "    device_id      STRING,\n" +
                "    temperature     FLOAT,\n" +
                "    timestamps     TIMESTAMP(4),\n" +
                "    PRIMARY KEY (device_id) NOT ENFORCED" +
                ") WITH (\n" +
                "    'connector'  = 'jdbc',\n" +
                "    'url'        = 'jdbc:postgresql://localhost:5432/ft',\n" +
                "    'table-name' = 'device_temperature',\n" +
                "    'driver'     = 'org.postgresql.Driver',\n" +
                "    'username'   = 'postgres',\n" +
                "    'password'   = 'postgres'\n" +
                ")");
        tableResult.print();

        //source
        DataStreamSource<String> inputStream = env.readTextFile(
                "/home/magic/workspace/flink-jobs/UDF/pythonUDF/src/main/resources/device_temperature.txt"
        );
        inputStream.print();
        env.execute("job");

//        tEnv.getConfig().getConfiguration().setString("python.files",
//                "/home/magic/workspace/python/flinkTestUdf/udfTest.py");
//        tEnv.getConfig().getConfiguration().setString("python.client.executable", "/usr/bin/python3");
//        tEnv.getConfig().getConfiguration().setString("python.executable", "/usr/bin/python3");
//        tEnv.getConfig().getConfiguration().setString("taskmanager.memory.task.off-heap.size", "79mb");
//        /*pass here the function.py and the name of the function into the python script*/
//        tEnv.executeSql(
//                "CREATE TEMPORARY SYSTEM FUNCTION add1 AS 'udfTest.add_one' LANGUAGE PYTHON"
//        );
//
//        TableResult tableResult = tEnv.executeSql("select add1(3)");
//        System.out.println(tableResult.toString());


    }
}
