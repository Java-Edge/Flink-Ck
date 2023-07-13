package com.javaedge.flink.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkKafkaApp {
    public static void main(String[] args) throws Exception {
        DataStream<String> stream = FlinkUtils.createKafkaStreamV2(args, SimpleStringSchema.class);
        stream.print();
        FlinkUtils.env.execute();

//        test01();
    }


    public static void test01() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka相关参数
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "");
        properties.setProperty("group.id", "test");
        properties.setProperty(" ", "false");
        properties.setProperty("auto.offset.reset", "earliest");
        String topic = "test_javaedge_10";

        // checkpoint相关参数
        env.enableCheckpointing(5000);

        env.setStateBackend(new FsStateBackend("file:///Users/rocky/Desktop/Flink/workspace/javaedge-flink/state"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.of(5, TimeUnit.SECONDS)));


        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(kafkaConsumer);
        stream.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] splits = value.split(",");
                        for (String split : splits) {
                            out.collect(split);
                        }
                    }
                }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                }).keyBy(x -> x.f0)
                .sum(1).print("wc统计:");


        env.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        if (value.contains("JavaEdge")) throw new RuntimeException("把JavaEdge哥拉黑");
                        return value.toUpperCase();
                    }
                }).print("from socket:");

        env.execute("FlinkKafkaApp");
    }
}
