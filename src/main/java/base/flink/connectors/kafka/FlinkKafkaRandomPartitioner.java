package base.flink.connectors.kafka;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.util.Preconditions;

public class FlinkKafkaRandomPartitioner<T> extends FlinkFixedPartitioner<T> {
    private static final long serialVersionUID = -3785320239953858777L;


    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        Preconditions.checkArgument(partitions != null && partitions.length > 0, "Partitions of the target topic is empty.");

        int index =  (int)(Math.random()*(partitions.length));

        return partitions[index];
    }
}
