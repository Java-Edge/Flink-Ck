package com.javaedge.flink.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class FlinkUtils {

    public static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static DataStream<String> createKafkaStreamV1(String[] args) throws Exception {
        ParameterTool tool = ParameterTool.fromPropertiesFile(args[0]);

        String groupId = tool.get("group.id", "test1");
        String servers = tool.getRequired("bootstrap.servers");
        List<String> topics = Arrays.asList(tool.getRequired("kafka.input.topics").split(","));
        String autoCommit = tool.get("enable.auto.commit", "false");
        String offsetReset = tool.get("auto.offset.reset", "earliest");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", groupId);
        properties.setProperty("enable.auto.commit", autoCommit);
        properties.setProperty("auto.offset.reset",offsetReset);


        int checkpointInterval = tool.getInt("checkpoint.interval", 5000);
        String checkpointPath = tool.get("checkpoint.path", "file:///Users/rocky/Desktop/Flink/workspace/javaedge-flink/state");

        env.enableCheckpointing(checkpointInterval);
        env.setStateBackend(new FsStateBackend(checkpointPath));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.of(5, TimeUnit.SECONDS)));


        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topics, new SimpleStringSchema(), properties);

        return env.addSource(kafkaConsumer);
    }

    /**
     * 万一以后不是字符串或是自定义类型了咋办？所以要实现泛型版本
     */
    public static <T> DataStream<T> createKafkaStreamV3(String[] args, Class<? extends KafkaDeserializationSchema<T>> deser) throws Exception {
        ParameterTool tool = ParameterTool.fromPropertiesFile(args[0]);

        String groupId = tool.get("group.id", "test1");
        String servers = tool.getRequired("bootstrap.servers");
        List<String> topics = Arrays.asList(tool.getRequired("kafka.input.topics").split(","));
        String autoCommit = tool.get("enable.auto.commit", "false");
        String offsetReset = tool.get("auto.offset.reset", "earliest");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", groupId);
        properties.setProperty("enable.auto.commit", autoCommit);
        properties.setProperty("auto.offset.reset",offsetReset);


        int checkpointInterval = tool.getInt("checkpoint.interval", 5000);
        String checkpointPath = tool.get("checkpoint.path", "file:///Users/rocky/Desktop/Flink/workspace/javaedge-flink/state");

        env.enableCheckpointing(checkpointInterval);
        env.setStateBackend(new FsStateBackend(checkpointPath));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.of(5, TimeUnit.SECONDS)));


        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topics, deser.newInstance(), properties);

        return env.addSource(kafkaConsumer);
    }

    public static <T> DataStream<T> createKafkaStreamV2(String[] args, Class<? extends DeserializationSchema<T>> deser) throws Exception {
        ParameterTool tool = ParameterTool.fromPropertiesFile(args[0]);

        String groupId = tool.get("group.id", "test1");
        String servers = tool.getRequired("bootstrap.servers");
        List<String> topics = Arrays.asList(tool.getRequired("kafka.input.topics").split(","));
        String autoCommit = tool.get("enable.auto.commit", "false");
        String offsetReset = tool.get("auto.offset.reset", "earliest");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("group.id", groupId);
        properties.setProperty("enable.auto.commit", autoCommit);
        properties.setProperty("auto.offset.reset",offsetReset);


        int checkpointInterval = tool.getInt("checkpoint.interval", 5000);
        String checkpointPath = tool.get("checkpoint.path", "file:///Users/rocky/Desktop/Flink/workspace/javaedge-flink/state");

        env.enableCheckpointing(checkpointInterval);
        env.setStateBackend(new FsStateBackend(checkpointPath));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.of(5, TimeUnit.SECONDS)));


        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(topics, deser.newInstance(), properties);

        return env.addSource(kafkaConsumer);
    }



    public static void main(String[] args) throws Exception{
        ParameterTool tool = ParameterTool.fromPropertiesFile(args[0]);

        // 参数是分成2大类：1)必填 2)选填
        String groupId = tool.get("group.id", "test1");
        String servers = tool.getRequired("bootstrap.servers");

        System.out.println(groupId);
        System.out.println(servers);
    }
}
