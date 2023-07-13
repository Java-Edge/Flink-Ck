package com.javaedge.flink.kafka;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;

/**
 * 实现了KafkaDeserializationSchema接口
 * 将Kafka中的消息反序列化为Tuple2<String, String>类型的对象
 */
public class JavaEdgeKafkaDeserializationSchema implements KafkaDeserializationSchema<Tuple2<String, String>> {

    /**
     * 判断消息流是否结束
     */
    @Override
    public boolean isEndOfStream(Tuple2<String, String> nextElement) {
        return false;
    }

    /**
     * 将ConsumerRecord<byte[], byte[]>类型的消息记录反序列化为Tuple2<String, String>类型的对象
     * 在这里，我们将topic、partition和offset拼接成一个唯一的id
     * 然后将消息的value转换为UTF-8编码的字符串，并返回一个Tuple2对象。
     */
    @Override
    public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        String topic = record.topic();
        int partition = record.partition();
        long offset = record.offset();

        String id = topic + "_" + partition + "_" + offset;


        String value = new String(record.value(), StandardCharsets.UTF_8);

        return Tuple2.of(id, value);
    }

    /**
     * 指定返回的Tuple2<String, String>类型对象的TypeInformation。
     */
    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
        });
    }
}
