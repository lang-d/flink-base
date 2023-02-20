package base.flink.connectors.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

import java.io.File;
import java.util.*;

public class KafkaConnector<T> {

    // 连接地址
    private String bootstrapServers;

    // SSL根证书的路径,使用公网的时候需要用到的
    private String sslTruststoreLocation;

    // 根证书store的密码，保持不变
    private String sslTruststorePassword = "KafkaOnsClient";

    private String javaSecurityAuthLoginConfig;

    // 接入协议,内网使用"PLAINTEXT",外网使用SASL_SSL
    private String securityProtocol;

    // SASL鉴权方式，保持不变
    private String saslMechanism = "PLAIN";

    // 消息key的序列化方式
    private String keySerializerClass;

    // 消息value的序列化方式
    private String valueSerializerClass;

    // 请求的最长等待时间
    private int maxBlockMs = 30 * 1000;

    // 重试次数
    private int retries = 5;

    // 重试间隔
    private int reconnectBackoffMs = 3000;

    // hostname 校验,改为空就可以了
    private String sslEndpointIdentificationAlgorithm = "";

    // 自定义的发送规则
    private FlinkKafkaPartitioner<T> customPartitioner;

    // 自定义的序列化schema
    private SerializationSchema<T> serialization;

    // acks
    // http://www.dengshenyu.com/kafka-producer/
    private String acks;

    private String compresstionType;

    private Integer batchSize;

    private Integer lingerMs;

    private Integer maxInFlightRequestsPerConnection;

    // timeout.ms, request.timeout.ms, metadata.fetch.timeout.ms
    private Integer timeoutMs;
    private Integer requestTimeoutMs;
    private Integer metadataFetchTimeoutMs;

    private Integer maxRequestSize;


    // receive.buffer.bytes, send.buffer.bytes
    private Integer receiveBufferBytes;
    private Integer sendBufferBytes;


    // ======== consumer 会用到的一些配置 =================
    //两次poll之间的最大允许间隔
    //可更加实际拉去数据和客户的版本等设置此值，默认30s
    private int sessionTimeoutMs = 30000;

    //设置单次拉取的量，走公网访问时，该参数会有较大影响
    private int maxPartitionFetchBytes = 32000;
    private int fetchMaxBytes = 32000;

    //每次poll的最大数量
    //注意该值不要改得太大，如果poll太多数据，而不能在下次poll之前消费完，则会触发一次负载均衡，产生卡顿
    private int maxPollRecords = 30;

    //消息的反序列化方式
    private String keyDeserializerClass;
    private String valueDeserializerClass;

    private String topic;

    private List<String> topics;

    private String consumerGroup;

    // 自定义的返序列化schema
    private DeserializationSchema<T> deserialization;


    private Properties props;

    public KafkaConnector<T> setCustomPartitioner(FlinkKafkaPartitioner<T> customPartitioner) {
        this.customPartitioner = customPartitioner;
        return this;
    }

    public KafkaConnector<T> setAcks(String acks) {
        this.acks = acks;
        return this;
    }

    public KafkaConnector<T> setCompresstionType(String compresstionType) {
        this.compresstionType = compresstionType;
        return this;
    }

    public KafkaConnector<T> setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public KafkaConnector<T> setLingerMs(int lingerMs) {
        this.lingerMs = lingerMs;
        return this;
    }

    public KafkaConnector<T> setMaxInFlightRequestsPerConnection(int maxInFlightRequestsPerConnection) {
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
        return this;
    }

    public KafkaConnector<T> setTimeoutMs(int timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public KafkaConnector<T> setRequestTimeoutMs(int requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
        return this;
    }

    public KafkaConnector<T> setMetadataFetchTimeoutMs(int metadataFetchTimeoutMs) {
        this.metadataFetchTimeoutMs = metadataFetchTimeoutMs;
        return this;
    }

    public KafkaConnector<T> setReceiveBufferBytes(int receiveBufferBytes) {
        this.receiveBufferBytes = receiveBufferBytes;
        return this;
    }

    public KafkaConnector<T> setSendBufferBytes(int sendBufferBytes) {
        this.sendBufferBytes = sendBufferBytes;
        return this;
    }

    public KafkaConnector<T> setMaxRequestSize(int maxRequestSize) {
        this.maxRequestSize = maxRequestSize;
        return this;
    }

    public KafkaConnector<T> setDeserialization(DeserializationSchema<T> deserialization) {
        this.deserialization = deserialization;
        return this;
    }

    public KafkaConnector<T> setSerialization(SerializationSchema<T> serialization) {
        this.serialization = serialization;
        return this;
    }


    public KafkaConnector<T> setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public KafkaConnector<T> setSslTruststoreLocation(String sslTruststoreLocation) {
        this.sslTruststoreLocation = sslTruststoreLocation;
        return this;
    }

    public KafkaConnector<T> setSslTruststorePassword(String sslTruststorePassword) {
        this.sslTruststorePassword = sslTruststorePassword;
        return this;
    }

    public KafkaConnector<T> setJavaSecurityAuthLoginConfig(String javaSecurityAuthLoginConfig) {
        this.javaSecurityAuthLoginConfig = javaSecurityAuthLoginConfig;
        return this;
    }

    /**
     * 外网的时候调用这个设置
     *
     * @return
     */
    public KafkaConnector<T> setSSLSecurityProtocol() {
        return this.setSecurityProtocol("SASL_SSL");
    }

    /**
     * 内网的时候调用这个
     *
     * @return
     */
    public KafkaConnector<T> setSecurityProtocol() {
        return this.setSecurityProtocol("PLAINTEXT");
    }

    public KafkaConnector<T> setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
        return this;
    }

    public KafkaConnector<T> setKeySerializerClass(String keySerializerClass) {
        this.keySerializerClass = keySerializerClass;
        return this;
    }

    public KafkaConnector<T> setValueSerializerClass(String valueSerializerClass) {
        this.valueSerializerClass = valueSerializerClass;
        return this;
    }

    public KafkaConnector<T> setMaxBlockMs(int maxBlockMs) {
        this.maxBlockMs = maxBlockMs;
        return this;
    }

    public KafkaConnector<T> setRetries(int retries) {
        this.retries = retries;
        return this;
    }

    public KafkaConnector<T> setReconnectBackoffMs(int reconnectBackoffMs) {
        this.reconnectBackoffMs = reconnectBackoffMs;
        return this;
    }

    public KafkaConnector<T> setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        return this;
    }

    public KafkaConnector<T> setMaxPartitionFetchBytes(int maxPartitionFetchBytes) {
        this.maxPartitionFetchBytes = maxPartitionFetchBytes;
        return this;
    }

    public KafkaConnector<T> setFetchMaxBytes(int fetchMaxBytes) {
        this.fetchMaxBytes = fetchMaxBytes;
        return this;
    }

    public KafkaConnector<T> setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
        return this;
    }

    public KafkaConnector<T> setKeyDeserializerClass(String keyDeserializerClass) {
        this.keyDeserializerClass = keyDeserializerClass;
        return this;
    }

    public KafkaConnector<T> setValueDeserializerClass(String valueDeserializerClass) {
        this.valueDeserializerClass = valueDeserializerClass;
        return this;
    }

    public KafkaConnector<T> setTopic(String topic) {
        this.topic = topic;
        if (this.topics == null) {
            this.topics = new ArrayList<>();
        }
        this.topics.add(topic);
        return this;
    }

    public KafkaConnector<T> setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
        return this;
    }

    public KafkaConnector<T> loadProperties(Properties props) throws Exception {
        if (props.getProperty("bootstrap.servers") != null) {
            this.bootstrapServers = props.getProperty("bootstrap.servers");
        } else {
            throw new Exception("not found kafka bootstrap.servers!");
        }

        if (props.getProperty("ssl.truststore.location") != null) {
            // 这里处理一下支持相对路径
            String path = Objects.requireNonNull(this.getClass().getClassLoader().getResource("")).getPath();
            String[] locationSplits = props.getProperty("ssl.truststore.location").split("\\\\");
            String fileName = locationSplits[locationSplits.length - 1];

            File f = new File(path + fileName);

            this.sslTruststoreLocation = f.getAbsolutePath();
        }

        if (props.getProperty("ssl.truststore.password") != null) {
            this.sslTruststorePassword = props.getProperty("ssl.truststore.password");
        }

        if (props.getProperty("security.protocol") != null) {
            this.securityProtocol = props.getProperty("security.protocol");
        }

        if (props.getProperty("sasl.mechanism") != null) {
            this.saslMechanism = props.getProperty("sasl.mechanism");
        }

        if (props.getProperty("key.serializer.class") != null) {
            this.keySerializerClass = props.getProperty("key.serializer.class");
        }

        if (props.getProperty("value.serializer.class") != null) {
            this.valueSerializerClass = props.getProperty("value.serializer.class");
        }

        if (props.getProperty("max.block.ms") != null) {
            this.maxBlockMs = Integer.parseInt(props.getProperty("max.block.ms"));
        }

        if (props.getProperty("retries") != null) {
            this.retries = Integer.parseInt(props.getProperty("retries"));
        }

        if (props.getProperty("reconnect.backoff.ms") != null) {
            this.reconnectBackoffMs = Integer.parseInt(props.getProperty("reconnect.backoff.ms"));
        }

        if (props.getProperty("ssl.endpoint.identification.algorithm") != null) {
            this.sslEndpointIdentificationAlgorithm = props.getProperty("ssl.endpoint.identification.algorithm");
        }

        if (props.getProperty("session.timeout.ms") != null) {
            this.sessionTimeoutMs = Integer.parseInt(props.getProperty("session.timeout.ms"));
        }

        if (props.getProperty("max.partition.fetch.bytes") != null) {
            this.maxPartitionFetchBytes = Integer.parseInt(props.getProperty("max.partition.fetch.bytes"));
        }

        if (props.getProperty("fetch.max.bytes") != null) {
            this.fetchMaxBytes = Integer.parseInt(props.getProperty("fetch.max.bytes"));
        }

        if (props.getProperty("max.poll.records") != null) {
            this.maxPollRecords = Integer.parseInt(props.getProperty("max.poll.records"));
        }

        if (props.getProperty("max.poll.records") != null) {
            this.maxPollRecords = Integer.parseInt(props.getProperty("max.poll.records"));
        }

        if (props.getProperty("key.deserializer.class") != null) {
            this.keyDeserializerClass = props.getProperty("key.deserializer.class");
        }

        if (props.getProperty("value.deserializer.class") != null) {
            this.valueDeserializerClass = props.getProperty("value.deserializer.class");
        }

        if (props.getProperty("consumer.group") != null) {
            this.consumerGroup = props.getProperty("consumer.group");
        }

        if (props.getProperty("java.security.auth.login.config") != null) {
            // 这里处理一下支持相对路径
            String path = Objects.requireNonNull(this.getClass().getClassLoader().getResource("")).getPath();
            String[] locationSplits = props.getProperty("java.security.auth.login.config").split("\\\\");
            String fileName = locationSplits[locationSplits.length - 1];

            File f = new File(path + fileName);

            this.javaSecurityAuthLoginConfig = f.getAbsolutePath();
        }

        if (props.getProperty("acks") != null) {
            this.acks = props.getProperty("acks");
        }

        if (props.getProperty("batch.size") != null) {
            this.batchSize = Integer.parseInt(props.getProperty("batch.size"));
        }

        if (props.getProperty("compression.type") != null) {
            this.compresstionType = props.getProperty("compression.type");
        }

        if (props.getProperty("linger.ms") != null) {
            this.lingerMs = Integer.parseInt(props.getProperty("linger.ms"));
        }

        if (props.getProperty("max.in.flight.requests.per.connection") != null) {
            this.maxInFlightRequestsPerConnection = Integer.parseInt(props.getProperty("max.in.flight.requests.per.connection"));
        }

        if (props.getProperty("timeout.ms") != null) {
            this.timeoutMs = Integer.parseInt(props.getProperty("timeout.ms"));
        }

        if (props.getProperty("request.timeout.ms") != null) {
            this.requestTimeoutMs = Integer.parseInt(props.getProperty("request.timeout.ms"));
        }

        if (props.getProperty("metadata.max.age.ms") != null) {
            this.metadataFetchTimeoutMs = Integer.parseInt(props.getProperty("metadata.max.age.ms"));
        }

        if (props.getProperty("receive.buffer.bytes") != null) {
            this.receiveBufferBytes = Integer.parseInt(props.getProperty("receive.buffer.bytes"));
        }

        if (props.getProperty("send.buffer.bytes") != null) {
            this.sendBufferBytes = Integer.parseInt(props.getProperty("send.buffer.bytes"));
        }

        if (props.getProperty("max.request.size") != null) {
            this.maxRequestSize = Integer.parseInt(props.getProperty("max.request.size"));
        }

        return this;
    }


    public KafkaConnector<T> loadProperties(String filePath) throws Exception {
        //获取配置文件kafka.properties的内容
        Properties props = new Properties();
        props.load(KafkaConnector.class.getClassLoader().getResourceAsStream(filePath));

        return loadProperties(props);

    }


    /**
     * 创建生产者属性
     *
     * @return
     */
    public Properties createProducerProperties() {
        Properties props = new Properties();
        //设置接入点，请通过控制台获取对应Topic的接入点
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        //设置SSL根证书的路径，请记得将XXX修改为自己的路径
        //与sasl路径类似，该文件也不能被打包到jar中
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, this.sslTruststoreLocation);

        //根证书store的密码，保持不变
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.sslTruststorePassword);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, this.securityProtocol);
        //SASL鉴权方式，保持不变
        props.put(SaslConfigs.SASL_MECHANISM, this.saslMechanism);
        //Kafka消息的序列化方式
        if (this.keySerializerClass != null) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, this.keyDeserializerClass);
        }

        if (this.valueSerializerClass != null) {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, this.valueSerializerClass);
        }

        // 如果有设置,以设置的为准
        if (this.javaSecurityAuthLoginConfig != null) {
            System.setProperty("java.security.auth.login.config", this.javaSecurityAuthLoginConfig);
        }

        //请求的最长等待时间
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, this.maxBlockMs);
        //设置客户端内部重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, this.retries);
        //设置客户端内部重试间隔
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, this.reconnectBackoffMs);

        //hostname校验改成空
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, this.sslEndpointIdentificationAlgorithm);

        if (this.acks != null) {
            props.put(ProducerConfig.ACKS_CONFIG, this.acks);
        }
        if (this.batchSize != null) {
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, this.batchSize);
        }
        if (this.compresstionType != null) {
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, this.compresstionType);
        }

        if (this.lingerMs != null) {
            props.put(ProducerConfig.LINGER_MS_CONFIG, this.lingerMs);
        }
        if (this.maxInFlightRequestsPerConnection != null) {
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, this.maxInFlightRequestsPerConnection);
        }

        if (this.requestTimeoutMs != null) {
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, this.requestTimeoutMs);
        }
        if (this.metadataFetchTimeoutMs != null) {
            props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, this.metadataFetchTimeoutMs);
        }
        if (this.receiveBufferBytes != null) {
            props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, this.receiveBufferBytes);
        }
        if (this.sendBufferBytes != null) {
            props.put(ProducerConfig.SEND_BUFFER_CONFIG, this.sendBufferBytes);
        }

        if (this.maxRequestSize != null) {
            props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, this.maxRequestSize);
        }


        return props;
    }

    /**
     * 创建消费者属性
     *
     * @return
     */
    public Properties createConsumerProperties() {
        Properties props = new Properties();

        //设置接入点，请通过控制台获取对应Topic的接入点
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);

        //设置SSL根证书的路径，请记得将XXX修改为自己的路径
        //与sasl路径类似，该文件也不能被打包到jar中
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, this.sslTruststoreLocation);
        //根证书store的密码，保持不变
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, this.sslTruststorePassword);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, this.securityProtocol);
        //SASL鉴权方式，保持不变
        props.put(SaslConfigs.SASL_MECHANISM, this.saslMechanism);

        // 如果有设置,以设置的为准
        if (this.javaSecurityAuthLoginConfig != null ) {
            System.setProperty("java.security.auth.login.config", this.javaSecurityAuthLoginConfig);
        }

        //hostname校验改成空
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, this.sslEndpointIdentificationAlgorithm);


        //两次poll之间的最大允许间隔
        //可更加实际拉去数据和客户的版本等设置此值，默认30s
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, this.sessionTimeoutMs);
        //设置单次拉取的量，走公网访问时，该参数会有较大影响
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, this.maxPartitionFetchBytes);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, this.fetchMaxBytes);
        //每次poll的最大数量
        //注意该值不要改得太大，如果poll太多数据，而不能在下次poll之前消费完，则会触发一次负载均衡，产生卡顿
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.maxPollRecords);
        //消息的反序列化方式
        if (this.keyDeserializerClass != null) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, this.keyDeserializerClass);
        }
        if (this.valueDeserializerClass != null) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, this.valueDeserializerClass);
        }
        if (this.consumerGroup != null) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, this.consumerGroup);
        }

        return props;
    }

    public KafkaSource<T> buildSource() {
        Preconditions.checkNotNull(this.topics);
        this.consumerGroup = Preconditions.checkNotNull(this.consumerGroup);
        Preconditions.checkNotNull(this.deserialization);

        Properties props = this.createConsumerProperties();

        return KafkaSource.<T>builder().setProperties(props).setTopics(this.topics).
                setValueOnlyDeserializer(deserialization).
                setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)).
                build();

    }


    public FlinkKafkaConsumer<T> buildConsumer() {
        Preconditions.checkNotNull(this.topics);
        this.consumerGroup = Preconditions.checkNotNull(this.consumerGroup);
        Preconditions.checkNotNull(this.deserialization);

        Properties props = this.createConsumerProperties();

        return new FlinkKafkaConsumer<T>(this.topics, deserialization, props);

    }

    public KafkaSource<String> buildSimpleStringSource() {
        Preconditions.checkNotNull(this.topics);
        this.consumerGroup = Preconditions.checkNotNull(this.consumerGroup);

        Properties props = this.createConsumerProperties();

        return KafkaSource.<String>builder().setProperties(props).setTopics(this.topics).
                setValueOnlyDeserializer(new SimpleStringSchema()).
                setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)).
                build();

    }

    public FlinkKafkaConsumer<String> buildSimpleStringConsumer() {
        Preconditions.checkNotNull(this.topics);
        this.consumerGroup = Preconditions.checkNotNull(this.consumerGroup);

        Properties props = this.createConsumerProperties();

        return new FlinkKafkaConsumer<String>(this.topics, new SimpleStringSchema(), props);

    }

    public FlinkKafkaProducer<T> buildProducer() {
        this.topic = Preconditions.checkNotNull(this.topic);
        Preconditions.checkNotNull(serialization);
        return new FlinkKafkaProducer<T>(topic, serialization, this.createProducerProperties(), Optional.ofNullable(customPartitioner));
    }

    public FlinkKafkaProducer<String> buildSimpleStringProducer() {
        this.topic = Preconditions.checkNotNull(this.topic);
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), this.createProducerProperties(),Optional.empty());
    }


}
