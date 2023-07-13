package com.javaedge.flink.app;

import com.alibaba.fastjson.JSON;
import com.javaedge.flink.domain.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 新老用户的统计分析
 *
 * 原来是根据数据中的某个字段
 * 现在我们是根据每个device来进行判断是否是新老用户
 *
 * 思考：device放到state里面去呢
 *
 * 我们的实现：状态 + 布隆过滤器
 *
 */
public class OsUserCntAppV3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<com.javaedge.flink.domain.Access> cleanStream = environment.readTextFile("data/access.json")
                .map(new MapFunction<String, com.javaedge.flink.domain.Access>() {
                    @Override
                    public com.javaedge.flink.domain.Access map(String value) throws Exception {
                        // TODO...  json ==> Access

                        try {
                            return JSON.parseObject(value, com.javaedge.flink.domain.Access.class);
                        } catch (Exception e) {
                            e.printStackTrace();
                            return null;
                        }

                    }
                }).filter(x -> x != null)
                .filter(new FilterFunction<com.javaedge.flink.domain.Access>() {
                    @Override
                    public boolean filter(com.javaedge.flink.domain.Access value) throws Exception {
                        return "startup".equals(value.event);
                    }
                });


        cleanStream.keyBy(x -> x.deviceType)
                .process(new KeyedProcessFunction<String, com.javaedge.flink.domain.Access, com.javaedge.flink.domain.Access>() {

                    private transient ValueState<BloomFilter<String>> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<BloomFilter<String>> descriptor = new ValueStateDescriptor<>("s", TypeInformation.of(new TypeHint<BloomFilter<String>>() {}));
                        state = getRuntimeContext().getState(descriptor);
                    }

                    /**
                     * 该代码是一个Flink流处理程序的一部分。它接收一个Access对象作为输入，并通过布隆过滤器判断设备是否已经存在。
                     * 如果设备不存在，则将设备添加到布隆过滤器中，并将nu属性设置为1，最后输出Access对象。
                     */
                    @Override
                    public void processElement(com.javaedge.flink.domain.Access value, Context ctx, Collector<Access> out) throws Exception {
                        /**
     *                      * 1. 获取Access对象的device属性。
     *                      * 2. 获取布隆过滤器的状态。
     *                      * 3. 如果布隆过滤器为空，则创建一个容量为100000的布隆过滤器。
     *                      * 4. 如果布隆过滤器中不存在该设备，则将设备添加到布隆过滤器中。
     *                      * 5. 将Access对象的nu属性设置为1。
     *                      * 6. 更新布隆过滤器的状态。
     *                      * 7. 输出Access对象。
                         */
                        String device = value.device;
                        BloomFilter<String> bloomFilter = state.value();
                        if(null == bloomFilter) {
                            bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 100000);
                        }

                        if(!bloomFilter.mightContain(device)) {
                            bloomFilter.put(device);
                            value.nu = 1;
                            state.update(bloomFilter);
                        }

                        out.collect(value);
                    }
                }).print();


        environment.execute("OsUserCntAppV2");

    }
}
