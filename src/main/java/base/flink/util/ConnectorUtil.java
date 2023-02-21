package base.flink.util;

import base.flink.connectors.kafka.FlinkKafkaRandomPartitioner;
import base.flink.connectors.kafka.KafkaConnector;
import base.flink.connectors.nedis.RedisConnector;
import base.flink.connectors.nedis.SimpleRedisMapper;
import cn.newrank.flink.connectors.redis.RedisSink;
import cn.newrank.flink.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class ConnectorUtil {

    public static FlinkKafkaConsumer<String> createStringConsumer(String propertiesPath, String topic, String consumerGroup) throws Exception {
        KafkaConnector<String> conn = new KafkaConnector<String>();
        conn.loadProperties(propertiesPath);
        conn.setTopic(topic);
        conn.setConsumerGroup(consumerGroup);

        return conn.buildSimpleStringConsumer();
    }

    public static KafkaSource<String> createStringSource(String propertiesPath, String topic, String consumerGroup) throws Exception {
        KafkaConnector<String> conn = new KafkaConnector<String>();
        conn.loadProperties(propertiesPath);
        conn.setTopic(topic);
        conn.setConsumerGroup(consumerGroup);

        return conn.buildSimpleStringSource();
    }

    public static FlinkKafkaProducer<String> createStringProducer(String propertiesPath, String topic) throws Exception {
        return new KafkaConnector<String>().
                loadProperties(propertiesPath).
                setTopic(topic).
                buildSimpleStringProducer();

    }

    public static FlinkKafkaProducer<String> createStringRandomProducer(String propertiesPath, String topic) throws Exception {
        return new KafkaConnector<String>().
                loadProperties(propertiesPath).
                setTopic(topic).
                setSerialization(new SimpleStringSchema()).
                setCustomPartitioner(new FlinkKafkaRandomPartitioner<>()).
                buildProducer();

    }

    public static FlinkKafkaProducer<String> createStringRandomProducerWithCompress(String propertiesPath, String topic,String compressType) throws Exception {
        return new KafkaConnector<String>().
                loadProperties(propertiesPath).
                setTopic(topic).
                setSerialization(new SimpleStringSchema()).
                setCustomPartitioner(new FlinkKafkaRandomPartitioner<>()).
                setCompresstionType(compressType).
                buildProducer();

    }

    public static FlinkKafkaProducer<String> createStringProducerWithCompress(String propertiesPath, String topic,String compressType) throws Exception {
        return new KafkaConnector<String>().
                loadProperties(propertiesPath).
                setTopic(topic).
                setCompresstionType(compressType).
                buildSimpleStringProducer();

    }

    public static RedisSink<Tuple2<String, String>> createSimpleRedisMapper(String propertiesPath, String additionKey, RedisCommand redisCommand, int ttl) throws Exception {
        return new RedisConnector<Tuple2<String, String>>().
                loadProperties(propertiesPath).
                setRedisMapper(new SimpleRedisMapper(additionKey, redisCommand, ttl)).
                createRedisSink();
    }

    public static RedisSink<Tuple2<String, String>> createSimpleRedisMapper(String propertiesPath, String additionKey, RedisCommand redisCommand) throws Exception {
        return new RedisConnector<Tuple2<String, String>>().
                loadProperties(propertiesPath).
                setRedisMapper(new SimpleRedisMapper(additionKey, redisCommand)).
                createRedisSink();
    }

}
