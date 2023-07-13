package com.javaedge.flink.app;

import com.javaedge.flink.domain.ItemInfo;
import com.javaedge.flink.domain.OrderInfo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 业务数据：订单、条目  存储在数据库中
 * <p>
 * mysql --> canal --> kafka --> flink
 */
public class FlinkJoinApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // order  0001,1000,300
        SingleOutputStreamOperator<OrderInfo> orderStream = environment.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, OrderInfo>() {
                    @Override
                    public OrderInfo map(String value) throws Exception {
                        String[] splits = value.split(",");

                        OrderInfo info = new OrderInfo();
                        info.orderId = splits[0].trim();
                        info.time = Long.parseLong(splits[1].trim());
                        info.money = Double.parseDouble(splits[2].trim());

                        return info;
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                        return element.time;
                                    }
                                }));


        // item    0001,1000,300,B,1,100
        SingleOutputStreamOperator<ItemInfo> itemStream = environment.socketTextStream("localhost", 9528)
                .map(new MapFunction<String, ItemInfo>() {
                    @Override
                    public ItemInfo map(String value) throws Exception {
                        String[] splits = value.split(",");
                        ItemInfo info = new ItemInfo();
                        info.itemId = Integer.parseInt(splits[0].trim());
                        info.orderId = splits[1].trim();
                        info.time = Long.parseLong(splits[2].trim());
                        info.sku = splits[3].trim();
                        info.amount = Double.parseDouble(splits[4].trim());
                        info.money = Double.parseDouble(splits[5].trim());

                        return info;
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<ItemInfo>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<ItemInfo>() {
                                    @Override
                                    public long extractTimestamp(ItemInfo element, long recordTimestamp) {
                                        return element.time;
                                    }
                                }));

        orderStream.print("order...");
        itemStream.print("order...");

        /**
         * 两个流的join操作
         *
         * item 作为左边， order作为右边
         * 理想化的：没有延迟
         *
         * (item, null) null就说明没有关联的订单而已 ==> API 查询业务库  RichXXXFunction
         * (迟到了,....)  对应于延迟到达这种item，就丢弃了...肯定不行！
         *     策略输出，放入 outputtag ==> item
         *     union
         *
         *
         *     item输入
         *     1,0001,1000,A,1,200
         *     2,0001,1000,B,1,100
         *     3,0003,7000,B,1,100
         *
         *     order 输入
         *     0001,1000,300
         *     0002,2000,400
         *     0003,3000 500
         *     0004,7000,500
         *
         */
        itemStream.coGroup(orderStream)
                .where(x -> x.orderId) // item
                .equalTo(y -> y.orderId)  // order
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<ItemInfo, OrderInfo, Tuple2<ItemInfo, OrderInfo>>() {
                    @Override
                    public void coGroup(Iterable<ItemInfo> first, Iterable<OrderInfo> second, Collector<Tuple2<ItemInfo, OrderInfo>> out) throws Exception {

                        for (ItemInfo itemInfo : first) {
                            boolean flag = false;
                            for (OrderInfo orderInfo : second) {
                                out.collect(Tuple2.of(itemInfo, orderInfo));
                                flag = true;
                            }

                            if (!flag) {
                                out.collect(Tuple2.of(itemInfo, null));
                            }
                        }
                    }
                }).print();

        environment.execute("FlinkJoinApp");
    }
}
