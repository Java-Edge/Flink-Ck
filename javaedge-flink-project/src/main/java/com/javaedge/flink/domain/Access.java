package com.javaedge.flink.domain;

import lombok.ToString;

@ToString
public class Access {

    public String device;
    public String deviceType;
    public String os;
    public String event;
    public String net;
    public String channel;
    public String uid;
    public int nu;  // 1新
    public int nu2;
    public String ip;  // ==> ip去解析
    public long time;
    public String version;
    public String province;
    public String city;

    public Product product;
    public String id;
    public String day;
    public String hour;
    public int ts;
}
