package base.flink.connectors.kafka;

import base.flink.pojo.JsonDetailsPojo;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@PublicEvolving
public class FlinkSafeKafkaJsonDeserializationSchema implements DeserializationSchema<JsonDetailsPojo> {

    private static final long serialVersionUID = 1509391548173891955L;

    private Logger LOG = LoggerFactory.getLogger(FlinkSafeKafkaJsonDeserializationSchema.class);

    @Override
    public JsonDetailsPojo deserialize(byte[] bytes) throws IOException {

        try {
            if (bytes != null) {
                JsonDetailsPojo json = JSONObject.parseObject(bytes, JsonDetailsPojo.class);
                json.setProcessTime();
                json.setPartition();
                return json;
            }
        } catch (Exception e) {
            LOG.error("deserialization json err", e);
        }

        return null;
    }

    public boolean isEndOfStream(JsonDetailsPojo nextElement) {
        return false;
    }

    public TypeInformation<JsonDetailsPojo> getProducedType() {
        return TypeExtractor.getForClass(JsonDetailsPojo.class);
    }
}
