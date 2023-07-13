package com.javaedge.flink.app;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

/**
 * CEP
 * <p>
 * 1) 定义模式
 * 2) 匹配结果
 * <p>
 * 需求：连续出现2次登录失败
 * 23  34
 */
public class CEPApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);


        KeyedStream<Access, String> stream = environment.fromElements(
                "001,78.89.90.2,success,1622689918",
                "002,110.111.112.113,failure,1622689952",
                "002,110.111.112.113,failure,1622689953",
                "002,110.111.112.113,failure,1622689954",
                "002,193.114.45.13,success,1622689959",
                "002,137.49.24.26,failure,1622689958"
        ).map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] splits = value.split(",");

                Access access = new Access();
                access.userId = splits[0];
                access.ip = splits[1];
                access.result = splits[2];
                access.time = Long.parseLong(splits[3]);

                return access;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Access>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(Access element) {
                return element.time * 1000;
            }
        }).keyBy(x -> x.userId);


        // 定义CEP规则
        /**
         * 这段代码的功能是定义一个模式，用于匹配满足特定条件的Access对象序列。
         *  代码步骤解释如下：
         * 1. 创建一个Pattern对象，该对象的输入类型和输出类型都是Access。
         * 2. 使用begin方法定义模式的起始状态，起始状态的名称为"start"。
         * 3. 使用where方法定义一个简单条件，该条件是一个SimpleCondition对象，用于过滤Access对象。在这个条件中，我们判断Access对象的result属性是否等于"failure"，如果是则返回true，否则返回false。
         * 4. 使用next方法定义模式的下一个状态，下一个状态的名称为"next"。
         * 5. 再次使用where方法定义一个简单条件，同样是判断Access对象的result属性是否等于"failure"。
         * 6. 使用within方法定义模式的时间限制，这里设置为3秒，表示在3秒内匹配到完整的模式。
         * 7. 最后，将定义好的模式赋值给pattern变量。
         * 总结：这段代码定义了一个模式，该模式要求在Access对象序列中，先出现一个result属性等于"failure"的对象，然后紧接着出现一个result属性也等于"failure"的对象，并且这两个对象的时间间隔不能超过3秒。
         */
        Pattern<Access, Access> pattern = Pattern.<Access>begin("start")
                .where(new SimpleCondition<Access>() {
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return value.result.equals("failure");
                    }
                }).next("next")
                .where(new SimpleCondition<Access>() {
                    @Override
                    public boolean filter(Access value) throws Exception {
                        return value.result.equals("failure");
                    }
                }).within(Time.seconds(3));


        // 把规则作用到数据流上
        PatternStream<Access> patternStream = CEP.pattern(stream, pattern);


        // 提取符合规则的数据
        patternStream.select(new PatternSelectFunction<Access, AccessMsg>() {
            @Override
            public AccessMsg select(Map<String, List<Access>> pattern) throws Exception {

                Access first = pattern.get("start").get(0);
                Access last = pattern.get("next").get(0);

                AccessMsg msg = new AccessMsg();
                msg.userId = first.userId;
                msg.first = first.time;
                msg.second = last.time;
                msg.msg = "出现连续登录失败...";

                return msg;
            }
        }).print();

        environment.execute("CEPApp");
    }
}


class AccessMsg {
    public String userId;
    public Long first;
    public Long second;
    public String msg;

    @Override
    public String toString() {
        return "AccessMsg{" +
                "userId='" + userId + '\'' +
                ", first=" + first +
                ", second=" + second +
                ", msg='" + msg + '\'' +
                '}';
    }
}

class Access {
    public String userId;
    public String ip;
    public String result;
    public Long time;
}
