package base.flink.connectors.kafka;

import base.flink.pojo.TaskMessagePojo;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

@PublicEvolving
public class FlinkKafkaTaskJsonDeserializationSchema implements DeserializationSchema<TaskMessagePojo> {

    private static final long serialVersionUID = 1509391548173891955L;


    @Override
    public TaskMessagePojo deserialize(byte[] bytes) throws IOException {
        if (bytes != null) {
            return JSONObject.parseObject(bytes, TaskMessagePojo.class);
        }
        return null;
    }

    public boolean isEndOfStream(TaskMessagePojo nextElement) {
        return false;
    }

    public TypeInformation<TaskMessagePojo> getProducedType() {
        return TypeExtractor.getForClass(TaskMessagePojo.class);
    }
}
