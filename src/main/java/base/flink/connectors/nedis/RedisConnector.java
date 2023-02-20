package base.flink.connectors.nedis;

import cn.newrank.flink.connectors.redis.RedisSink;
import cn.newrank.flink.connectors.redis.common.config.FlinkJedisPoolConfig;
import cn.newrank.flink.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Preconditions;

import java.util.Properties;

public class RedisConnector<T> {

    private String host;
    private Integer port;
    private String password;
    private Integer database;
    private RedisMapper<T> redisMapper;

    public RedisConnector() {
    }

    public RedisConnector(String host, Integer port, String password, Integer database) {
        this.host = host;
        this.port = port;
        this.password = password;
        this.database = database;
    }

    public RedisConnector<T> setHost(String host) {
        this.host = host;
        return this;
    }

    public RedisConnector<T> setPort(Integer port) {
        this.port = port;
        return this;
    }

    public RedisConnector<T> setPassword(String password) {
        this.password = password;
        return this;
    }

    public RedisConnector<T> setDatabase(Integer database) {
        this.database = database;
        return this;
    }

    public RedisConnector<T> loadProperties(String filePath) throws Exception {
        Properties props = new Properties();
        props.load(RedisConnector.class.getClassLoader().getResourceAsStream(filePath));

        return loadProperties(props);

    }

    public RedisConnector<T> loadProperties(Properties props) throws Exception {
        if (props.getProperty("host") == null) {
            throw new Exception("no host");
        }
        if (props.getProperty("port") == null) {
            throw new Exception("no port");
        }
        if (props.getProperty("password") == null) {
            throw new Exception("no password");
        }
        if (props.getProperty("database") == null) {
            throw new Exception("database");
        }

        this.host = props.getProperty("host");
        this.port = Integer.parseInt(props.getProperty("port"));
        this.password = props.getProperty("password");
        this.database = Integer.parseInt(props.getProperty("database"));

        return this;
    }

    public RedisConnector<T> setRedisMapper(RedisMapper<T> redisMapper) {
        this.redisMapper = redisMapper;
        return this;
    }

    public RedisSink<T> createRedisSink() {
        Preconditions.checkNotNull(this.host);
        Preconditions.checkNotNull(this.port);
        Preconditions.checkNotNull(this.password);
        Preconditions.checkNotNull(this.database);
        Preconditions.checkNotNull(this.redisMapper);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().
                setHost(this.host).
                setPort(this.port).
                setPassword(this.password).
                setDatabase(this.database).
                build();

        return new RedisSink<T>(conf, this.redisMapper);
    }

}
