package com.adv.flinkjobs.jobs.jython;

import org.python.core.PyObject;
import org.python.util.PythonInterpreter;
import java.io.FileInputStream;

public class ExecPython {
    public static void main(String[] args) throws Exception {
        PythonInterpreter pyInterp = new PythonInterpreter();
        String pyContent = "" +
                "def sum(a,b):\n" +
                "   return a+b\n" +
                "\n" +
                "if __name__ == '__main__':\n" +
                "   ret = sum(1,4)\n";


        System.out.println(pyContent);

        //exec string
        pyInterp.exec(pyContent);
        PyObject ret1 = pyInterp.get("ret");
        System.out.println("result1 = "+ret1);

        //exec file
        FileInputStream pyFile = new FileInputStream("/home/lz/workspace/flink-jobs/src/main/resources/pyTest.py");
        pyInterp.execfile(pyFile);
        PyObject ret2 = pyInterp.get("ret");
        System.out.println("result2 = "+ret2);
    }
}
