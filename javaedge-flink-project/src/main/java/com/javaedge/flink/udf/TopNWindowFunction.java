package com.javaedge.flink.udf;

import com.javaedge.flink.domain.EventCategoryProductCount;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 自定义的窗口函数，在Flink流处理中计算每个窗口内的Top N事件类别和产品数量。
 */
public class TopNWindowFunction implements WindowFunction<Long, EventCategoryProductCount, Tuple3<String, String, String>, TimeWindow> {

    /**
     * @param value The key for which this window is evaluated.
     * @param window The window that is being evaluated.
     * @param input The elements in the window being evaluated.
     * @param out A collector for emitting elements.
     * @throws Exception
     */
    @Override
    public void apply(Tuple3<String, String, String> value, TimeWindow window, Iterable<Long> input, Collector<EventCategoryProductCount> out) throws Exception {

        /**
         * 获取输入数据中的事件、类别、产品和数量
         */
        String event = value.f0;
        String category = value.f1;
        String product = value.f2;
        Long count = input.iterator().next();

        /**
         * 获取窗口的开始时间和结束时间。
         */
        long start = window.getStart();
        long end = window.getEnd();

        // 使用获取到的数据创建一个新的EventCategoryProductCount对象
        // 将新创建的对象添加到Collector中。
        // 这个窗口函数将被应用于一个时间窗口，并且输出的结果将包含事件、类别、产品、数量以及窗口的开始和结束时间。
        out.collect(new EventCategoryProductCount(event, category, product, count, start, end));
    }
}
