package com.javaedge.flink.domain;

import lombok.Data;
import lombok.ToString;

/**
 * @author JavaEdge
 * @date 2023/5/27
 */
@Data
@ToString
public class OrderInfo {

    // 作为 join 的条件
    public String orderId;
    public long time;
    public double money;
}
