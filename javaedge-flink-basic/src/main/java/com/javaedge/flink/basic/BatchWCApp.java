package com.javaedge.flink.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Flink批处理
 */
public class BatchWCApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = environment.readTextFile("data/wc.data");

        source.flatMap(new JavaEdgeFlatMapFunction())
                .map(new JavaEdgeMapFunction())
                .groupBy(0)
                .sum(1)
                .print();

    }
}

class JavaEdgeFlatMapFunction implements FlatMapFunction<String, String> {

    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        String[] words = value.split(",");
        for (String word : words) {
            out.collect(word.toLowerCase().trim());
        }
    }
}

class JavaEdgeMapFunction implements MapFunction<String, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(String value) throws Exception {
        return new Tuple2<>(value, 1);
    }
}