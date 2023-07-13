package com.javaedge.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

public class SourceApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

//        environment.readCsvFile("data/people.csv")
//                .fieldDelimiter(";")
//                .ignoreFirstLine()
//                .types(String.class, Integer.class, String.class)
//                .print();

//        environment.readTextFile("data/wc.txt").print();

        // codec
//        environment.readTextFile("data/wc.txt.gz").print();

        Configuration configuration = new Configuration();
        configuration.setBoolean("recursive.file.enumeration", true);

        environment.readTextFile("data/nest")
                .withParameters(configuration)
                .print();
    }
}
