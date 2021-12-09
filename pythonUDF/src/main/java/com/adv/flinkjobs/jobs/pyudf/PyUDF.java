package com.adv.flinkjobs.jobs.pyudf;

import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class PyUDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //set cfg
        tEnv.getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, 1);
        tEnv.getConfig().getConfiguration().set(PythonOptions.USE_MANAGED_MEMORY, true);
        tEnv.getConfig().getConfiguration().setString("python.files",
                "file:///home/magic/workspace/flink-jobs/UDF/pythonUDF/src/main/resources/udfTest.py");
        tEnv.getConfig().getConfiguration().setString("python.client.executable",
                "/usr/bin/python3");
        tEnv.getConfig().getConfiguration().setString("python.executable",
                "/usr/bin/python3");

        tEnv.executeSql(
                "CREATE TEMPORARY SYSTEM FUNCTION add_one AS 'udfTest.add_one' LANGUAGE PYTHON"
        );
        tEnv.createTemporaryView("source", tEnv.fromValues(1, 2, 3).as("a"));

        System.out.println("table source has 3 ");

        Iterator<Row> result = tEnv.executeSql("select add_one(a) as a from source").collect();

        List<Integer> actual = new ArrayList<>();
        while (result.hasNext()) {
            Row r = result.next();
            System.out.println(""+r.toString());
            actual.add((Integer) r.getField(0));
        }

        List<Integer> expected = Arrays.asList(2, 3, 4);
        if (!actual.equals(expected)) {
            throw new AssertionError(String.format("The output result: %s is not as expected: %s!", actual, expected));
        }
    }
}
