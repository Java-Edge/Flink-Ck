package com.javaedge.flink.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * 两阶段提交参考文档：
 * https://www.ververica.com/blog/end-to-end-exactly-once-processing-apache-flink-apache-kafka
 */
public class FlinkKafka2RedisApp {
    public static void main(String[] args) throws Exception {
        DataStream<String> stream = FlinkUtils.createKafkaStreamV2(args, SimpleStringSchema.class);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = stream.flatMap(new FlatMapFunction<String, String>() {
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
                .sum(1);

        result.print();


        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        result.addSink(new RedisSink<Tuple2<String, Integer>>(conf, new RedisExampleMapper()));

        FlinkUtils.env.execute();
    }

    static class RedisExampleMapper implements RedisMapper<Tuple2<String, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "wc");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1 + "";
        }

    }
}
