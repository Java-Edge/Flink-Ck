package com.javaedge.flink.kafka;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

public class KafkaTest {
    public static void main(String[] args) throws Exception {
        DataStream<Tuple2<String, String>> source = FlinkUtils.createKafkaStreamV3(args,
                JavaEdgeKafkaDeserializationSchema.class);
        source.print();
        FlinkUtils.env.execute();
    }
}
