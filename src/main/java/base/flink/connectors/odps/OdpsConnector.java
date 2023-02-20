package base.flink.connectors.odps;

import base.flink.connectors.kafka.KafkaConnector;
import com.alibaba.ververica.connectors.common.sink.TupleOutputFormatSinkFunction;
import com.alibaba.ververica.connectors.odps.OdpsCompressOption;
import com.alibaba.ververica.connectors.odps.sink.OdpsOutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.alibaba.ververica.connectors.continuous.odps.source.ContinuousODPSStreamSource;
import com.alibaba.ververica.connectors.odps.ODPSStreamSource;
import com.alibaba.ververica.connectors.odps.OdpsConf;
import com.alibaba.ververica.connectors.odps.OdpsOptions;
import com.alibaba.ververica.connectors.odps.schema.ODPSColumn;
import com.alibaba.ververica.connectors.odps.schema.ODPSTableSchema;
import com.alibaba.ververica.connectors.odps.util.OdpsUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class OdpsConnector {
    private Logger LOG = LoggerFactory.getLogger(OdpsConnector.class);

    private String endpoint;

    // vpc下必填
    private String tunnelEndpoint;

    private String project;

    private String tablename;

    private String accessid;

    private String accesskey;

    private String startPartition;

    /**
     * 如果存在分区表，则必填partition。填写partition需要注意以下两点：
     * 固定分区
     * 例如`partition` = 'ds=20180905'表示将数据写入分区ds= 20180905。
     *
     * 动态分区
     * 如果未明文显示分区值，则根据写入数据中分区列具体值，写入对应分区。例如`partition`='ds'表示根据ds字段值写入对应分区。
     *
     * 如果要创建多级动态分区，Partition中多个字段顺序必须和MaxCompute物理表保持一致，各个分区字段之间使用逗号（,）分割。
     */
    private String partition;

    private Long retryTimes;

    private Long sleepTimesMs;

    private Long maxPartitionCount;

    /**
     * 默认值为100，系统会把已写入的分区和TunnelBufferedWriter的映射关系维护到一个Map里，
     * 如果该Map大小超过了dynamicPartitionLimit设定值，则会出现Too many dynamic partitions: 100, which exceeds the size limit: 100报错。
     */
    private Integer dynamicPartitionLimit;

    /**
     * Odps Tunnel Writer缓冲区Flush间隔。
     * MaxCompute Sink写入记录时，先将数据存储到MaxCompute的缓冲区中，
     * 等缓冲区溢出或者每隔一段时间（flushIntervalMs），再把缓冲区里的数据写到目标 MaxCompute表。
     */
    private Long flushIntervalMs;

    private String cacheReloadTimeBlackList;

    private Integer subscribeIntervalInSec;

    private Integer maxLoadRetries;

    private Integer fetchCount;

    // MaxCompute Tunnel Writer缓冲区Flush的大小。
    //MaxCompute Sink写入记录时，先将数据存储到MaxCompute的缓冲区中，等缓冲区达到一定大小（batchSize），再把缓冲区里的数据写到目标MaxCompute表。
    private Long batchSize;

    //MaxCompute Tunnel Writer缓冲区Flush的线程数。
    //每个MaxCompute Sink并发将创建numFlushThreads个线程用于flush数据。当该值大于1时，将允许不同分区的数据并发Flush，提升Flush的效率。
    // 默认值为1。
    private Integer numFlushThreads;

    // MaxCompute Tunnel使用的压缩算法。
    // 参数取值如下：
    //  RAW（无压缩）
    //  ZLIB
    //  SNAPPY
    //相比ZLIB，SNAPPY能带来明显的吞吐提升。在测试场景下，吞吐提升约50%。
    //说明
    //仅实时计算引擎VVR 4.0.13及以上版本支持该参数。
    //VVR 4.0.13版本及以上版本，该参数默认值为ZLIB; VVR 6.0.1及以上版本，该参数默认值为SNAPPY。
    private String compressAlgorithm;

    // 是否使用MaxCompute Stream Tunnel上传数据。
    // 参数取值如下：
    //  true：使用MaxCompute Stream Tunnel上传数据。
    //  false（默认值）：使用MaxCompute Batch Tunnel上传数据。
    //对于使用MaxCompute Batch Tunnel的作业，在Checkpoint进行的很慢甚至超时，且确认下游可以接受重复数据时，可以考虑使用MaxCompute Stream Tunnel。
    private Boolean useStreamTunnel;


    public Long getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Long batchSize) {
        this.batchSize = batchSize;
    }

    public Integer getNumFlushThreads() {
        return numFlushThreads;
    }

    public void setNumFlushThreads(Integer numFlushThreads) {
        this.numFlushThreads = numFlushThreads;
    }

    public String getCompressAlgorithm() {
        return compressAlgorithm;
    }

    public void setCompressAlgorithm(String compressAlgorithm) {
        this.compressAlgorithm = compressAlgorithm;
    }

    public Boolean getUseStreamTunnel() {
        return useStreamTunnel;
    }

    public void setUseStreamTunnel(Boolean useStreamTunnel) {
        this.useStreamTunnel = useStreamTunnel;
    }

    public String getPartition() {
        return partition;
    }

    public OdpsConnector setPartition(String partition) {
        this.partition = partition;
        return this;
    }

    public Long getRetryTimes() {
        return retryTimes;
    }

    public OdpsConnector setRetryTimes(Long retryTimes) {
        this.retryTimes = retryTimes;
        return this;
    }

    public Long getSleepTimesMs() {
        return sleepTimesMs;
    }

    public OdpsConnector setSleepTimesMs(Long sleepTimesMs) {
        this.sleepTimesMs = sleepTimesMs;
        return this;
    }

    public Long getMaxPartitionCount() {
        return maxPartitionCount;
    }

    public OdpsConnector setMaxPartitionCount(Long maxPartitionCount) {
        this.maxPartitionCount = maxPartitionCount;
        return this;
    }


    public Integer getDynamicPartitionLimit() {
        return dynamicPartitionLimit;
    }

    public void setDynamicPartitionLimit(Integer dynamicPartitionLimit) {
        this.dynamicPartitionLimit = dynamicPartitionLimit;
    }

    public Long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public OdpsConnector setFlushIntervalMs(Long flushIntervalMs) {
        this.flushIntervalMs = flushIntervalMs;
        return this;
    }

    public String getCacheReloadTimeBlackList() {
        return cacheReloadTimeBlackList;
    }

    public OdpsConnector setCacheReloadTimeBlackList(String cacheReloadTimeBlackList) {
        this.cacheReloadTimeBlackList = cacheReloadTimeBlackList;
        return this;
    }

    public Integer getSubscribeIntervalInSec() {
        return subscribeIntervalInSec;
    }

    public OdpsConnector setSubscribeIntervalInSec(Integer subscribeIntervalInSec) {
        this.subscribeIntervalInSec = subscribeIntervalInSec;
        return this;
    }

    public Integer getMaxLoadRetries() {
        return maxLoadRetries;
    }

    public OdpsConnector setMaxLoadRetries(Integer maxLoadRetries) {
        this.maxLoadRetries = maxLoadRetries;
        return this;
    }

    public Integer getFetchCount() {
        return fetchCount;
    }

    public OdpsConnector setFetchCount(Integer fetchCount) {
        this.fetchCount = fetchCount;
        return this;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public OdpsConnector setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public String getTunnelEndpoint() {
        return tunnelEndpoint;
    }

    public OdpsConnector setTunnelEndpoint(String tunnelEndpoint) {
        this.tunnelEndpoint = tunnelEndpoint;
        return this;
    }

    public String getProject() {
        return project;
    }

    public OdpsConnector setProject(String project) {
        this.project = project;
        return this;
    }

    public String getTablename() {
        return tablename;
    }

    public OdpsConnector setTablename(String tablename) {
        this.tablename = tablename;
        return this;
    }

    public String getAccessid() {
        return accessid;
    }

    public OdpsConnector setAccessid(String accessid) {
        this.accessid = accessid;
        return this;
    }

    public String getAccesskey() {
        return accesskey;
    }

    public OdpsConnector setAccesskey(String accesskey) {
        this.accesskey = accesskey;
        return this;
    }

    public String getStartPartition() {
        return startPartition;
    }

    public OdpsConnector setStartPartition(String startPartition) {
        this.startPartition = startPartition;
        return this;
    }

    public OdpsConnector loadProperties(Properties props) throws Exception {

        if(props.getProperty("endpoint")!=null){
            this.endpoint = props.getProperty("endpoint");
        }
        if(props.getProperty("tunnelEndpoint")!=null){
            this.tunnelEndpoint = props.getProperty("tunnelEndpoint");
        }
        if(props.getProperty("project")!=null){
            this.project = props.getProperty("project");
        }
        if(props.getProperty("tablename")!=null){
            this.tablename = props.getProperty("tablename");
        }
        if(props.getProperty("accessid")!=null){
            this.accessid = props.getProperty("accessid");
        }
        if(props.getProperty("accesskey")!=null){
            this.accesskey = props.getProperty("accesskey");
        }
        if(props.getProperty("startPartition")!=null){
            this.startPartition = props.getProperty("startPartition");
        }
        if(props.getProperty("partition")!=null){
            this.partition = props.getProperty("partition");
        }
        if(props.getProperty("retryTimes")!=null){
            this.retryTimes = Long.parseLong(props.getProperty("retryTimes"));
        }
        if(props.getProperty("sleepTimesMs")!=null){
            this.sleepTimesMs = Long.parseLong(props.getProperty("sleepTimesMs"));
        }
        if(props.getProperty("maxPartitionCount")!=null){
            this.maxPartitionCount = Long.parseLong(props.getProperty("maxPartitionCount"));
        }
        if(props.getProperty("dynamicPartitionLimit")!=null){
            this.dynamicPartitionLimit = Integer.parseInt(props.getProperty("dynamicPartitionLimit"));
        }
        if(props.getProperty("flushIntervalMs")!=null){
            this.flushIntervalMs = Long.parseLong(props.getProperty("flushIntervalMs"));
        }
        if(props.getProperty("cacheReloadTimeBlackList")!=null){
            this.cacheReloadTimeBlackList = props.getProperty("cacheReloadTimeBlackList");
        }
        if(props.getProperty("subscribeIntervalInSec")!=null){
            this.subscribeIntervalInSec = Integer.parseInt(props.getProperty("subscribeIntervalInSec"));
        }
        if(props.getProperty("maxLoadRetries")!=null){
            this.maxLoadRetries = Integer.parseInt(props.getProperty("maxLoadRetries"));
        }
        if(props.getProperty("fetchCount")!=null){
            this.fetchCount = Integer.parseInt(props.getProperty("fetchCount"));
        }

        if(props.getProperty("batchSize")!=null){
            this.batchSize = Long.parseLong(props.getProperty("batchSize"));
        }
        if(props.getProperty("numFlushThreads")!=null){
            this.numFlushThreads = Integer.parseInt(props.getProperty("numFlushThreads"));
        }
        if(props.getProperty("compressAlgorithm")!=null){
            this.compressAlgorithm = props.getProperty("compressAlgorithm");
        }
        if(props.getProperty("useStreamTunnel")!=null){
            this.useStreamTunnel = Boolean.getBoolean(props.getProperty("useStreamTunnel"));
        }


        return this;
    }


    public OdpsConnector loadProperties(String filePath) throws Exception {
        //获取配置文件odps.properties的内容
        Properties props = new Properties();
        props.load(KafkaConnector.class.getClassLoader().getResourceAsStream(filePath));

        return loadProperties(props);

    }


    /**
     * 构建source配置
     * @return
     */
    public Configuration buildConfiguration(){
        Configuration conf = new Configuration();
        conf.setString(OdpsOptions.END_POINT.key(), getEndpoint());
        conf.setString(OdpsOptions.PROJECT_NAME.key(), getProject());
        conf.setString(OdpsOptions.TABLE_NAME.key(), getTablename());
        conf.setString(OdpsOptions.ACCESS_ID.key(), getAccessid());
        conf.setString(OdpsOptions.ACCESS_KEY.key(), getAccesskey());

        if(getStartPartition()!=null){
            conf.setString(OdpsOptions.START_PARTITION.key(), getStartPartition());
        }

        if(getTunnelEndpoint()!=null){
            conf.setString(OdpsOptions.TUNNEL_END_POINT.key(),getTunnelEndpoint());
        }

        if(getPartition()!=null){
            conf.setString(OdpsOptions.PARTITION.key(),getPartition());
        }

        if(getRetryTimes()!=null){
            conf.setString(OdpsOptions.RETRY_TIME.key(),getRetryTimes().toString());
        }
        if(getSleepTimesMs()!=null){
            conf.setString(OdpsOptions.SLEEP_MILLIS.key(),getSleepTimesMs().toString());
        }
        if(getMaxPartitionCount()!=null){
            conf.setString(OdpsOptions.MAX_PARTITION_COUNT.key(),getMaxPartitionCount().toString());
        }
        if(getDynamicPartitionLimit()!=null){
            conf.setString(OdpsOptions.DYNAMIC_PART_LIMIT.key(),getDynamicPartitionLimit().toString());
        }
        if(getFlushIntervalMs()!=null){
            conf.setString(OdpsOptions.FLUSH_INTERVAL_MS_CONF.key(),getFlushIntervalMs().toString());
        }
        if(getCacheReloadTimeBlackList()!=null){
            conf.setString(OdpsOptions.OPTIONAL_CACHE_RELOAD_TIME_BLACKLIST.key(),getCacheReloadTimeBlackList());
        }
        if(getSubscribeIntervalInSec()!=null){
            conf.setString(OdpsOptions.SUBSCRIBE_INTERVAL_IN_SEC.key(),getSubscribeIntervalInSec().toString());
        }
        if(getMaxLoadRetries()!=null){
            conf.setString(OdpsOptions.MAX_RELOAD_RETRIES.key(),getMaxLoadRetries().toString());
        }
        if(getFetchCount()!=null){
            conf.setString(OdpsOptions.FETCH_COUNT.key(),getFetchCount().toString());
        }
        if(getCompressAlgorithm()!=null){
            switch (getCompressAlgorithm()){
                case "RAW":
                    conf.set(OdpsOptions.COMPRESS_ALGORITHM, OdpsCompressOption.Algorithm.RAW);
                    break;
                case "ZLIB":
                    conf.set(OdpsOptions.COMPRESS_ALGORITHM, OdpsCompressOption.Algorithm.ZLIB);
                    break;
                case "SNAPPY":
                    conf.set(OdpsOptions.COMPRESS_ALGORITHM, OdpsCompressOption.Algorithm.SNAPPY);
                    break;
                default:
                    LOG.warn("not support COMPRESS_ALGORITHM:{}",getCompressAlgorithm());
                    conf.set(OdpsOptions.COMPRESS_ALGORITHM, OdpsOptions.COMPRESS_ALGORITHM.defaultValue());
            }

        }

        return conf;
    }

    /**
     * 构建sink配置
     * @return
     */
    public DescriptorProperties buildProperties(){
        DescriptorProperties properties = new DescriptorProperties();
        properties.putString(OdpsOptions.END_POINT.key(), getEndpoint());
        properties.putString(OdpsOptions.PROJECT_NAME.key(), getProject());
        properties.putString(OdpsOptions.TABLE_NAME.key(), getTablename());
        properties.putString(OdpsOptions.ACCESS_ID.key(), getAccessid());
        properties.putString(OdpsOptions.ACCESS_KEY.key(), getAccesskey());

        if(getTunnelEndpoint()!=null){
            properties.putString(OdpsOptions.TUNNEL_END_POINT.key(),getTunnelEndpoint());
        }

        if(getPartition()!=null){
            properties.putString(OdpsOptions.PARTITION.key(),getPartition());
        }

        if(getDynamicPartitionLimit()!=null){
            properties.putString(OdpsOptions.DYNAMIC_PART_LIMIT.key(),getDynamicPartitionLimit().toString());
        }
        if(getFlushIntervalMs()!=null){
            properties.putString(OdpsOptions.FLUSH_INTERVAL_MS_CONF.key(),getFlushIntervalMs().toString());
        }
        if(getBatchSize()!=null){
            properties.putLong(OdpsOptions.BATCH_SIZE.key(),getBatchSize());
        }
        if(getNumFlushThreads()!=null){
            properties.putInt(OdpsOptions.NUM_FLUSH_THREADS.key(),getNumFlushThreads());
        }
        if(getCompressAlgorithm()!=null){
            switch (getCompressAlgorithm()){
                case "RAW":
                    properties.putClass(OdpsOptions.COMPRESS_ALGORITHM.key(), OdpsCompressOption.Algorithm.RAW.getClass());
                    break;
                case "ZLIB":
                    properties.putClass(OdpsOptions.COMPRESS_ALGORITHM.key(), OdpsCompressOption.Algorithm.ZLIB.getClass());
                    break;
                case "SNAPPY":
                    properties.putClass(OdpsOptions.COMPRESS_ALGORITHM.key(), OdpsCompressOption.Algorithm.SNAPPY.getClass());
                    break;
                default:
                    LOG.warn("not support COMPRESS_ALGORITHM:{}",getCompressAlgorithm());
                    properties.putClass(OdpsOptions.COMPRESS_ALGORITHM.key(), OdpsOptions.COMPRESS_ALGORITHM.defaultValue().getClass());
            }
            properties.putInt(OdpsOptions.COMPRESS_LEVEL.key(),OdpsOptions.COMPRESS_LEVEL.defaultValue());
            properties.putInt(OdpsOptions.COMPRESS_STRATEGY.key(),OdpsOptions.COMPRESS_STRATEGY.defaultValue());

        }

        if(getUseStreamTunnel()!=null){
            properties.putBoolean(OdpsOptions.USE_STREAMING_TUNNEL.key(),getUseStreamTunnel());
        }


        return properties;
    }

    /**
     * 创建sink
     * @param schema
     * @return
     */
    public TupleOutputFormatSinkFunction<Row> buildSink(TableSchema schema){
        return new TupleOutputFormatSinkFunction<>(
                new OdpsOutputFormat(schema, buildProperties()));
    }

    /**
     * 创建普通source
     * @param schema
     * @return
     */
    public ODPSStreamSource buildSource(TableSchema schema){
        return new OdpsSourceBuilder(schema, buildConfiguration()).buildSourceFunction();
    }

    /**
     * 创建增量source
     * @param schema
     * @return
     */
    public ContinuousODPSStreamSource buildContinuousSource(TableSchema schema){
        return new OdpsSourceBuilder(schema, buildConfiguration()).buildContinuousOdpsSource();
    }

    /**
     * source 构建器
     */
    public static class OdpsSourceBuilder {

        private final OdpsConf odpsConf;
        private final String startPartition;
        private final String odpsTable;

        private TypeInformation<RowData> producedTypeInfo;
        private ODPSColumn[] selectedColumns;

        private long retryTimes;
        private long sleepTimesMs;
        private long maxPartitionCount;
        private int discoveryIntervalInMs;
        private List<String> prunedPartitions;

        private OdpsCompressOption compressOption;

        public OdpsSourceBuilder(
                TableSchema tableSchema,
                Configuration conf) {
            this.odpsConf = OdpsUtils.createOdpsConf(conf);
            this.startPartition = conf.getString(OdpsOptions.START_PARTITION);
            this.odpsTable = conf.getString(OdpsOptions.TABLE_NAME);
            String specificPartition = conf.getString(OdpsOptions.PARTITION);
            ODPSTableSchema physicalTableSchema = OdpsUtils.getOdpsTableSchema(odpsConf, odpsTable);

            boolean isPartitionedTable = physicalTableSchema.isPartition();
            this.maxPartitionCount = conf.getInteger(OdpsOptions.MAX_PARTITION_COUNT);
            this.prunedPartitions = getSpecificPartitions(
                    odpsConf, odpsTable, isPartitionedTable, specificPartition);

            Preconditions.checkArgument(
                    isPartitionedTable || StringUtils.isEmpty(startPartition),
                    "Non-partitioned table can not be an unbounded source.");

            this.producedTypeInfo = InternalTypeInfo.of(tableSchema.toRowDataType().getLogicalType());
            this.selectedColumns = OdpsUtils.validateAndGetProjectCols(physicalTableSchema, tableSchema);

            this.discoveryIntervalInMs = conf.getInteger(OdpsOptions.SUBSCRIBE_INTERVAL_IN_SEC);
            this.retryTimes = conf.getInteger(OdpsOptions.RETRY_TIME);
            this.sleepTimesMs = conf.getInteger(OdpsOptions.SLEEP_MILLIS);

            // compressOption
            compressOption = new OdpsCompressOption(conf.get(OdpsOptions.COMPRESS_ALGORITHM),
                    conf.getInteger(OdpsOptions.COMPRESS_LEVEL),
                    conf.getInteger(OdpsOptions.COMPRESS_STRATEGY));


        }

        public ODPSStreamSource buildSourceFunction() {
            // OdpsConf odpsConf, String tableName, ODPSColumn[] selectedColumns,
            // List<String> partitions, OdpsCompressOption compressOption,
            // TypeInformation<RowData> rowDataTypeInfo, long sleepTimeMs, long retryTimes
            return new ODPSStreamSource(
                    odpsConf,
                    odpsTable,
                    selectedColumns,
                    prunedPartitions,
                    compressOption,
                    producedTypeInfo,
                    sleepTimesMs,
                    retryTimes);
        }

        public ContinuousODPSStreamSource buildContinuousOdpsSource() {
            // OdpsConf odpsConf, String tableName, ODPSColumn[] selectedColumns,
            // TypeInformation<RowData> rowDataTypeInfo, OdpsCompressOption compressOption,
            // long sleepTimeMs, long retryTimes, String startPartition, int subscribeIntervalSec
            return new ContinuousODPSStreamSource(
                    odpsConf,
                    odpsTable,
                    selectedColumns,
                    producedTypeInfo,
                    compressOption,
                    sleepTimesMs,
                    retryTimes,
                    startPartition,
                    discoveryIntervalInMs);
        }

        private List<String> getSpecificPartitions(
                OdpsConf odpsConf,
                String odpsTable,
                boolean isPartitionedTable,
                String specificPartition) {
            if (!isPartitionedTable) {
                return new ArrayList<>();
            }
            List<String> conditions = new ArrayList<>();
            if (StringUtils.isNotEmpty(specificPartition)) {
                conditions.add(specificPartition);
            }
            List<String> partitions = OdpsUtils.getMatchedPartitions(
                    odpsConf, odpsTable, conditions, true, true);

            if (partitions.size() > maxPartitionCount) {
                throw new TableException(
                        String.format(
                                "The number of matched partitions [%d] exceeds"
                                        + " the default limit of [%d]! \nPlease confirm whether you need to read all these "
                                        + "partitions, if you really need it, you can increase the `maxPartitionCount` "
                                        + "in DDL's with options.",
                                partitions.size(), maxPartitionCount));
            }
            return partitions;
        }
    }
}
