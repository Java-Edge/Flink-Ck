package com.javaedge.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.ArrayList;
import java.util.List;

public class SinkApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        List<Integer> info = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            info.add(i);
        }

//        DataSource<Integer> data = environment.fromCollection(info);
//        System.out.println(data.getParallelism());

        environment.setParallelism(3);
        DataSource<Long> data = environment.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);

        data.writeAsText("out", FileSystem.WriteMode.OVERWRITE);

        environment.execute("SinkApp");
    }
}
