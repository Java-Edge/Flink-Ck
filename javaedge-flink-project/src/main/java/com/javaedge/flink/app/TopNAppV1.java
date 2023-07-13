package com.javaedge.flink.app;

import com.alibaba.fastjson.JSON;
import com.javaedge.flink.domain.Access;
import com.javaedge.flink.domain.EventCategoryProductCount;
import com.javaedge.flink.udf.TopNAggregateFunction;
import com.javaedge.flink.udf.TopNWindowFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TopNAppV1 {
    public static void main(String[] args) throws Exception {
        // 创建一个StreamExecutionEnvironment对象env，用于执行流处理任务
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 这段代码的功能是读取一个名为"access.json"的文件，并将文件中的内容转换为Access对象。然后，过滤掉为null的对象
         * 并为每个Access对象分配时间戳和水印。
         * 最后，再次过滤掉event字段为"startup"的Access对象。
         */
        // 使用env.readTextFile("data/access.json")读取名为"access.json"的文件，并返回一个DataStream<String>对象
        SingleOutputStreamOperator<Access> cleanStream = env.readTextFile("data/access.json")
                // 对DataStream<String>对象进行map操作，将每个字符串转换为Access对象。在转换过程中，使用JSON.parseObject(value, Access.class)将字符串解析为Access对象
                .map((MapFunction<String, Access>) value -> {
                    try {
                        return JSON.parseObject(value, Access.class);
                    } catch (Exception e) {
                        // 如果解析过程中发生异常，会打印异常信息，并返回null。
                        e.printStackTrace();
                        return null;
                    }
                })
                // 过滤掉空的Access对象
                .filter(Objects::nonNull)
                // 为每个Access对象分配时间戳和水印
                .assignTimestampsAndWatermarks(
                        // 设置了一个20s的时间窗口
                        new BoundedOutOfOrdernessTimestampExtractor<Access>(Time.seconds(20)) {
                            // 重写方法，将Access对象的time字段作为时间戳。
                            @Override
                            public long extractTimestamp(Access element) {
                                return element.time;
                            }
                        }
                )
                // 过滤掉event字段为"startup"的Access对象
                .filter((FilterFunction<Access>) value -> {
                    return !"startup".equals(value.event);
                });

        WindowedStream<Access, Tuple3<String, String, String>, TimeWindow> windowStream = cleanStream
                .keyBy(new KeySelector<Access, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(Access value) throws Exception {
                        return Tuple3.of(value.event, value.product.category, value.product.name);
                    }
                })
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)));

        SingleOutputStreamOperator<EventCategoryProductCount> aggStream = windowStream.aggregate(
                new TopNAggregateFunction(),
                new TopNWindowFunction());

        aggStream.keyBy(new KeySelector<EventCategoryProductCount, Tuple4<String, String, Long, Long>>() {
            @Override
            public Tuple4<String, String, Long, Long> getKey(EventCategoryProductCount value) throws Exception {
                return Tuple4.of(value.event, value.category, value.start, value.end);
            }
        }).process(new KeyedProcessFunction<Tuple4<String, String, Long, Long>, EventCategoryProductCount, List<EventCategoryProductCount>>() {

            private transient ListState<EventCategoryProductCount> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                listState = getRuntimeContext().getListState(new ListStateDescriptor<EventCategoryProductCount>("cnt-state", EventCategoryProductCount.class));
            }

            @Override
            public void processElement(EventCategoryProductCount value, Context ctx, Collector<List<EventCategoryProductCount>> out) throws Exception {
                listState.add(value);

                // 注册一个定时器
                ctx.timerService().registerEventTimeTimer(value.end + 1);
            }


            // 在这里完成TopN操作
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<EventCategoryProductCount>> out) throws Exception {
                ArrayList<EventCategoryProductCount> list = Lists.newArrayList(listState.get());

                list.sort((x, y) -> Long.compare(y.count, x.count));

                ArrayList<EventCategoryProductCount> sorted = new ArrayList<>();

                for (int i = 0; i < Math.min(3, list.size()); i++) {
                    EventCategoryProductCount bean = list.get(i);
                    sorted.add(bean);
                }

                out.collect(sorted);
            }
        }).print().setParallelism(1);

        env.execute();
    }

}
