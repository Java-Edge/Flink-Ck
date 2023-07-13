package com.javaedge.flink.domain;

import lombok.Data;
import lombok.ToString;

/**
 * @author JavaEdge
 * @date 2023/5/27
 */
@ToString
@Data
public class ItemInfo {
    public int itemId;
    // 作为 join 的条件
    public String orderId;
    public long time;
    public String sku;
    public double amount;
    public double money;
}
