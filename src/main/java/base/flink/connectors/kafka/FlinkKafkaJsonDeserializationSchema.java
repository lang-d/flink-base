package base.flink.connectors.kafka;

import base.flink.pojo.JsonDetailsPojo;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

@PublicEvolving
public class FlinkKafkaJsonDeserializationSchema implements DeserializationSchema<JsonDetailsPojo> {

    private static final long serialVersionUID = 1509391548173891955L;

    @Override
    public JsonDetailsPojo deserialize(byte[] bytes) throws IOException {

        if (bytes != null) {
            JsonDetailsPojo json = JSONObject.parseObject(bytes, JsonDetailsPojo.class);
            json.setProcessTime();
            json.setPartition();
            return json;
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
