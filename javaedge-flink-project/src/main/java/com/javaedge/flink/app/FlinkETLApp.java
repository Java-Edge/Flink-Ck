package com.javaedge.flink.app;

import com.alibaba.fastjson.JSON;
import com.javaedge.flink.domain.Access;
import com.javaedge.flink.kafka.FlinkUtils;
import com.javaedge.flink.kafka.JavaEdgeKafkaDeserializationSchema;
import com.javaedge.flink.udf.GaodeLocationMapFunction;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;

public class FlinkETLApp {

    public static void main(String[] args) throws Exception {
        DataStream<Tuple2<String, String>> source = FlinkUtils.createKafkaStreamV3(args, JavaEdgeKafkaDeserializationSchema.class);

        FastDateFormat format = FastDateFormat.getInstance("yyyyMMdd-HH");

        source.map(new MapFunction<Tuple2<String, String>, Access>() {
                    @Override
                    public Access map(Tuple2<String, String> value) throws Exception {
                        // 注意事项：一定要考虑解析的容错性
                        try {

                            Access bean = JSON.parseObject(value.f1, Access.class);
                            bean.id = value.f0;

                            long time = bean.time;
                            String[] splits = format.format(time).split("-");
                            String day = splits[0];
                            String hour = splits[1];
                            bean.day = day;
                            bean.hour = hour;

                            return bean;
                        } catch (Exception e) {
                            e.printStackTrace(); // 写到某个地方
                            return null;
                        }
                    }
                }).filter(x -> x != null)
                .filter(new FilterFunction<Access>() {
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return !"startup".equals(value.event);
                    }
                })
                //.map(new GaodeLocationMapFunctionV2())
                .addSink(JdbcSink.sink(
                        "insert into ch_event1  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (pstmt, x) -> {
                            pstmt.setString(1, x.id);
                            pstmt.setString(2, x.device);
                            pstmt.setString(3, x.deviceType);
                            pstmt.setString(4, x.os);
                            pstmt.setString(5, x.event);
                            pstmt.setString(6, x.net);
                            pstmt.setString(7, x.channel);
                            pstmt.setString(8, x.uid);
                            pstmt.setInt(9, x.nu);
                            pstmt.setString(10, x.ip);
                            pstmt.setLong(11, x.time);
                            pstmt.setString(12, x.version);
                            pstmt.setString(13, x.province);
                            pstmt.setString(14, x.city);
                            pstmt.setString(15, x.day);
                            pstmt.setString(16, x.hour);
                            pstmt.setLong(17, System.currentTimeMillis());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1)
                                .withBatchIntervalMs(4000).build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://10.51.51.48:8123/default")
                                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                                .withUsername("default")
                                .withPassword("icv@2022")
                                .build()
                ));
        FlinkUtils.env.execute();
    }
}
