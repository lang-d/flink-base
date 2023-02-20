package base.flink.connectors.nedis;

import cn.newrank.flink.connectors.redis.common.mapper.RedisCommand;
import cn.newrank.flink.connectors.redis.common.mapper.RedisCommandDescription;
import cn.newrank.flink.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Optional;

public class SimpleRedisMapper implements RedisMapper<Tuple2<String, String>> {

    private String additionalKey;

    private RedisCommand redisCommand = RedisCommand.SET;

    // 过期时间
    private Integer ttl;

    public SimpleRedisMapper(String additionKey) {
        this.additionalKey = additionKey;
    }

    public SimpleRedisMapper(int ttl) {
        this.ttl = ttl;
    }

    public SimpleRedisMapper(String additionKey, RedisCommand redisCommand) {
        this.additionalKey = additionKey;
        this.redisCommand = redisCommand;
    }

    public SimpleRedisMapper(String additionKey, RedisCommand redisCommand, int ttl) {
        this.additionalKey = additionKey;
        this.redisCommand = redisCommand;
        this.ttl = ttl;
    }

    public SimpleRedisMapper(String additionKey, int ttl) {
        this.additionalKey = additionKey;
        this.ttl = ttl;
    }

    @Override
    public RedisCommandDescription getCommandDescription() {
        if (this.ttl != null) {
            if (this.additionalKey != null) {
                return new RedisCommandDescription(this.redisCommand, this.additionalKey, this.ttl);
            }
            return new RedisCommandDescription(this.redisCommand, this.ttl);
        }
        if (this.additionalKey != null) {
            return new RedisCommandDescription(this.redisCommand, this.additionalKey);
        }
        return new RedisCommandDescription(this.redisCommand);
    }

    @Override
    public String getKeyFromData(Tuple2<String, String> stringStringTuple2) {
        return stringStringTuple2.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, String> stringStringTuple2) {
        return stringStringTuple2.f1;
    }

    @Override
    public Optional<Integer> getAdditionalTTL(Tuple2<String, String> data) {
        if (this.ttl != null) {
            return Optional.of(this.ttl);
        } else {
            return Optional.empty();
        }
    }
}
