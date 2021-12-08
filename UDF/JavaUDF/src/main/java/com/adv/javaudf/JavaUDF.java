package com.adv.javaudf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

public class JavaUDF {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        //Here is the upcoming new features.
        //https://cwiki.apache.org/confluence/display/FLINK/FLIP-79+Flink+Function+DDL+Support
        //https://cwiki.apache.org/confluence/display/FLINK/FLIP-178+Support+Advanced+Function+DDL
        //It provides us an official way to adding remote files/jars.

        String path = "https://api-apprepo-sundi-ensaas.axa.wise-paas.com.cn/v1.0/filerepo/others/win_exe_app/files/jarlib-1.0.0.jar";
        loadJar(new URL(path));

        Field configuration = StreamExecutionEnvironment.class.getDeclaredField("configuration");
        configuration.setAccessible(true);
        Configuration o = (Configuration)configuration.get(env);

        Field confData = Configuration.class.getDeclaredField("confData");
        confData.setAccessible(true);
        Map<String,Object> temp = (Map<String,Object>)confData.get(o);
        List<String> jarList = new ArrayList<>();
        jarList.add(path);
        temp.put("pipeline.classpaths",jarList);

        //set cfg
        tEnv.getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, 1);

        String createFunctionSql = "CREATE TEMPORARY SYSTEM FUNCTION substring1 " +
                "AS 'com.adv.udf.Substring'";
        System.out.println(createFunctionSql);

        tEnv.executeSql(createFunctionSql);

        tEnv.createTemporaryView("source", tEnv.fromValues("abcdefg", "1234567", "9876543").as("a"));

        System.out.println("table source has 3 lines \n abcdefg\n1234567\n9876543");

        Iterator<Row> result = tEnv.executeSql("select substring1(a,1,5) as a from source").collect();

        List<String> actual = new ArrayList<>();
        while (result.hasNext()) {
            Row r = result.next();
            System.out.println("" + r.toString());
            actual.add(r.getField(0).toString());
        }

        List<String> expected = Arrays.asList("bcde", "2345", "8765");
        if (!actual.equals(expected)) {
            throw new AssertionError(String.format("The output result: %s is not as expected: %s!", actual, expected));
        }
    }

    //动态加载Jar
    public static void loadJar(URL jarUrl) {
        //从URLClassLoader类加载器中获取类的addURL方法
        Method method = null;
        try {
            method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
        } catch (NoSuchMethodException | SecurityException e1) {
            e1.printStackTrace();
        }
        // 获取方法的访问权限
        boolean accessible = method.isAccessible();
        try {
            //修改访问权限为可写
            if (accessible == false) {
                method.setAccessible(true);
            }
            // 获取系统类加载器
            URLClassLoader classLoader = (URLClassLoader)Thread.currentThread().getContextClassLoader();
            //jar路径加入到系统url路径里
            method.invoke(classLoader, jarUrl);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            method.setAccessible(accessible);
        }
    }
}
