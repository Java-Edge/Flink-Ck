package com.javaedge.flink.app;

import com.alibaba.fastjson.JSON;
import com.javaedge.flink.domain.Access;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Objects;

/**
 * 按照操作系统维度进行新老用户的统计分析
 */
@Slf4j
public class OsUserCntAppV1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Access> cleanStream = environment.readTextFile("data/access.json")
                .map(new MapFunction<String, com.javaedge.flink.domain.Access>() {
                    @Override
                    public Access map(String value) throws Exception {
                        // TODO...  json ==> Access

                        // 考虑解析的容错性，毕竟总有非法的数据
                        try {
                            return JSON.parseObject(value, Access.class);
                        } catch (Exception e) {
                            log.error("error=", e);
                            // 异常数据就返回空
                            return null;
                        }

                    }
                })
                // 过滤上面 catch 到的非法的处理后为空的数据
                .filter(Objects::nonNull)
                .filter(new FilterFunction<com.javaedge.flink.domain.Access>() {

                    @Override
                    public boolean filter(com.javaedge.flink.domain.Access value) throws Exception {
                        // 启动日志
                        return "startup".equals(value.event);
                    }
                });

        // TODO... 操作系统维度  新老用户  ==> wc
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> result = cleanStream.map(new MapFunction<Access, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(Access value) throws Exception {
                return Tuple3.of(value.os, value.nu, 1);
            }
        }).keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        }).sum(2);// .print().setParallelism(1);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();

        result.addSink(new RedisSink<>(conf, new RedisExampleMapper()));

        environment.execute("OsUserCntAppV1");

    }


    static class RedisExampleMapper implements RedisMapper<Tuple3<String, Integer, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "os-user-cnt:20301002");
        }

        @Override
        public String getKeyFromData(Tuple3<String, Integer, Integer> data) {
            return data.f0 + "_" + data.f1;
        }

        @Override
        public String getValueFromData(Tuple3<String, Integer, Integer> data) {
            return data.f2 + "";
        }

    }
}
