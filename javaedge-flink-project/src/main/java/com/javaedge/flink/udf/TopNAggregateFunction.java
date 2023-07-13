package com.javaedge.flink.udf;

import com.javaedge.flink.domain.Access;
import org.apache.flink.api.common.functions.AggregateFunction;

public class TopNAggregateFunction implements AggregateFunction<Access, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Access value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return null;
    }
}
