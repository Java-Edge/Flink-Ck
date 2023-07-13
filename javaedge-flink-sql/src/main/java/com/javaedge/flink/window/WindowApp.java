package com.javaedge.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class WindowApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        /**
         * 窗口大小10s，wm允许是0
         *
         * 0000-9999  JavaEdge: 205  zs:6
         * 10000-19999 JavaEdge:45
         *
         */
        SingleOutputStreamOperator<Tuple4<Long, String, String, Double>> input = env.fromElements( // 时间(eventtime),用户名,购买的商品,价格
                "1000,JavaEdge,spark,75",
                "2000,JavaEdge,flink,65",
                "2000,zs,kuangquanshui,3",
                "3000,JavaEdge,cdh,65",
                "9999,zs,xuebi,3",
                "19999,JavaEdge,Hive,45"
        ).map(new MapFunction<String, Tuple4<Long, String, String, Double>>() {
            @Override
            public Tuple4<Long, String, String, Double> map(String value) throws Exception {
                String[] splits = value.split(",");
                Long time = Long.parseLong(splits[0]);
                String user = splits[1];
                String book = splits[2];
                Double money = Double.parseDouble(splits[3]);
                return Tuple4.of(time, user, book, money);
            }
        }).assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Tuple4<Long, String, String, Double>>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Tuple4<Long, String, String, Double> element) {
                        return element.f0;
                    }
                }
        );


        Table table = tableEnv.fromDataStream(input, $("time"), $("user_id"), $("book"), $("money"), $("rowtime").rowtime());

        Table resultTable = table.window(Tumble.over(lit(10).seconds()).on($("rowtime")).as("win"))
                .groupBy($("user_id"), $("win"))
                .select($("user_id"), $("money").sum().as("total"), $("win").start(), $("win").end());

        tableEnv.toRetractStream(resultTable, Row.class).filter(x -> x.f0).print();


//        tableEnv.createTemporaryView("access", input, $("time"), $("user_id"), $("book"), $("money"), $("rowtime").rowtime());
//
//        Table resultTable = tableEnv.sqlQuery("select TUMBLE_START(rowtime, interval '10' second) as win_start,TUMBLE_END(rowtime, interval '10' second) as win_end, user_id, sum(money) from access group by TUMBLE(rowtime, interval '10' second), user_id");
//        tableEnv.toRetractStream(resultTable, Row.class).filter(x -> x.f0).print();

        env.execute("WindowApp");

    }
}
