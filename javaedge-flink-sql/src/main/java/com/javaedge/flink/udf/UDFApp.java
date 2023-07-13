package com.javaedge.flink.udf;
import static org.apache.flink.table.api.Expressions.$;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 需求：自定义UDF函数解析IP信息
 * 输入：ip
 * 输出：省份-城市
 */
public class UDFApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> input = env.socketTextStream("localhost", 9527);
        tableEnv.createTemporaryView("access", input, $("ip"));

        tableEnv.createTemporaryFunction("ip_parser", new IPParser());

        Table resultTable = tableEnv.sqlQuery("select ip, ip_parser(ip) from access");
        tableEnv.toAppendStream(resultTable, Row.class).print();


        env.execute("UDFApp");
    }

}
