package base.flink.connectors.holo;

import com.alibaba.ververica.connectors.hologres.api.AbstractHologresWriter;
import com.alibaba.ververica.connectors.hologres.api.HologresTableSchema;
import com.alibaba.ververica.connectors.hologres.config.HologresConfigs;
import com.alibaba.ververica.connectors.hologres.config.HologresConnectionParam;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCConfigs;
import com.alibaba.ververica.connectors.hologres.jdbc.HologresJDBCWriter;
import com.alibaba.ververica.connectors.hologres.rpc.HologresRpcWriter;
import com.alibaba.ververica.connectors.hologres.sink.HologresSinkFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;

/**
 * 提供hologres 入库的工具方法
 */
public class HologresFormat implements Serializable {

    private Logger LOG = LoggerFactory.getLogger(HologresFormat.class);

    private static final long serialVersionUID = 0L;

    private HologresProperties props;

    private String[] fieldList;

    private DataType[] dataTypeList;

    private HashMap<String, DataType> dataTypeMap;

    private String PrimaryKey;

    private TableSchema tableSchema;

    private String tableSql;


    public HologresFormat(String[] fieldList, DataType[] dataTypeList, HologresProperties props) {
        this.fieldList = fieldList;
        this.dataTypeList = dataTypeList;
        this.props = props;

        this.setDataTypeMap(dataTypeList);

        this.ckeckDataStruct();

        this.tableSchema = this.createTableScheme();

        this.tableSql = this.createTableSql();

    }

    public HologresFormat(String[] fieldList, HashMap<String, DataType> dataTypeMap, HologresProperties props) {
        this.fieldList = fieldList;
        this.dataTypeMap = dataTypeMap;
        this.props = props;

        this.setDataTypeList(dataTypeMap);

        this.ckeckDataStruct();

        this.tableSchema = this.createTableScheme();

        this.tableSql = this.createTableSql();

    }

    private void setDataTypeMap(DataType[] dataTypeList) {
        this.dataTypeMap = new HashMap<String, DataType>(this.fieldList.length);
        for (int i = 0; i < fieldList.length; i++) {
            this.dataTypeMap.put(this.fieldList[i], dataTypeList[i]);
        }
    }

    private void setDataTypeList(HashMap<String, DataType> dataTypeMap) {
        this.dataTypeList = new DataType[this.fieldList.length];

        for (int i = 0; i < fieldList.length; i++) {
            this.dataTypeList[i] = dataTypeMap.get(fieldList[i]);
        }
    }

    private void ckeckDataStruct() {
        for (String s : fieldList) {
            DataType type = dataTypeMap.get(s);
            if (type == null) {
                LOG.error(String.format("field:%s not set dataType", s));
                System.exit(1);
            }
        }
    }

    public Configuration createConfiguration(HologresProperties props) {

        Configuration config = new Configuration();

        config.setString(HologresConfigs.ENDPOINT, props.getEndpoint());
        config.setString(HologresConfigs.USERNAME, props.getUserName());
        config.setString(HologresConfigs.PASSWORD, props.getPassword());
        config.setString(HologresConfigs.DATABASE, props.getDataBase());
        config.setString(HologresConfigs.TABLE, props.getTableName());

        config.setBoolean(HologresConfigs.ENABLE_PARTITION_TABLE, props.isPartitionrouter());
        switch (this.props.getMutatetype()) {
            case HologresProperties.INSERT_OR_IGNORE:
                config.setString(HologresConfigs.MUTATE_TYPE, HologresProperties.INSERT_OR_IGNORE);
                break;
            case HologresProperties.INSERT_OR_REPLACE:
                config.setString(HologresConfigs.MUTATE_TYPE, HologresProperties.INSERT_OR_REPLACE);
                break;
            case HologresProperties.INSERT_OR_UPDATE:
                config.setString(HologresConfigs.MUTATE_TYPE, HologresProperties.INSERT_OR_UPDATE);
                break;
            default:
                config.setString(HologresConfigs.MUTATE_TYPE, HologresProperties.INSERT_OR_IGNORE);
        }
        if (props.isIgnorNullWhenUpdate()) {
            config.setBoolean(HologresConfigs.IGNORE_NULL_WHEN_UPDATE, props.isIgnorNullWhenUpdate());
        }

        config.setBoolean(HologresConfigs.CREATE_MISSING_PARTITION_TABLE, props.isCreateparttable());

        config.setBoolean(HologresConfigs.USE_RPC_MODE, props.isUseRpcMode());


        if (props.getConnectionSize() != null) {
            config.setInteger(HologresJDBCConfigs.OPTIONAL_CLIENT_CONNECTION_POOL_SIZE, props.getConnectionSize());
        }

        if (props.getJdbcWriteBatchSize() != null) {
            config.setInteger(HologresJDBCConfigs.OPTIONAL_JDBC_WRITE_BATCH_SIZE, props.getJdbcWriteBatchSize());
        }

        if (props.getJdbcWriteFlushInterval() != null) {
            config.setLong(HologresJDBCConfigs.OPTIONAL_JDBC_WRITE_FLUSH_INTERVAL, props.getJdbcWriteFlushInterval());
        }

        if(props.getConnectionPoolName()!=null){
            config.setString(HologresJDBCConfigs.OPTIONAL_JDBC_SHARED_CONNECTION_POOL_NAME,props.getConnectionPoolName());
        }
        if(props.getJdbcWriteBatchByteSize()!=null){
            config.setLong(HologresJDBCConfigs.OPTIONAL_JDBC_WRITE_BATCH_BYTE_SIZE,props.getJdbcWriteBatchByteSize());
        }

        // 因为默认是true,所以如果不允许的话,要配置一下
        if(!props.isJdbcEnableDefaultForNotNullColumn()){
            config.setBoolean(HologresJDBCConfigs.OPTIONAL_JDBC_ENABLE_DEFAULT_FOR_NOT_NULL_COLUMN,props.isJdbcEnableDefaultForNotNullColumn());
        }
        if(props.getJdbcRetryCount()!=null){
            config.setInteger(HologresJDBCConfigs.OPTIONAL_JDBC_RETRY_COUNT,props.getJdbcRetryCount());
        }
        if(props.getJdbcRetrySleepInitMs()!=null){
            config.setLong(HologresJDBCConfigs.OPTIONAL_JDBC_RETRY_SLEEP_INIT_MS,props.getJdbcRetrySleepInitMs());
        }
        if(props.getJdbcRetrySleepStepMs()!=null){
            config.setLong(HologresJDBCConfigs.OPTIONAL_JDBC_RETRY_SLEEP_STEP_MS,props.getJdbcRetrySleepStepMs());
        }
        if(props.getJdbcConnectionMaxIdleMs()!=null){
            config.setLong(HologresJDBCConfigs.OPTIONAL_JDBC_CONNECTION_MAX_IDLE_MS,props.getJdbcConnectionMaxIdleMs());
        }
        if(props.getJdbcMetaCacheTTL()!=null){
            config.setLong(HologresJDBCConfigs.OPTIONAL_JDBC_META_CACHE_TTL,props.getJdbcMetaCacheTTL());
        }
        if(props.getJdbcMetaAutoRefreshFactor()!=null){
            config.setInteger(HologresJDBCConfigs.OPTIONAL_JDBC_META_AUTO_REFRESH_FACTOR,props.getJdbcMetaAutoRefreshFactor());
        }
        if(props.getIgnoredelete()!=null){
            config.setBoolean(HologresConfigs.OPTIONAL_SINK_IGNORE_DELETE,props.getIgnoredelete());
        }

        return config;


    }


    public TableSchema getTableSchema() {
        return this.tableSchema;
    }

    public String getTableSql() {
        return this.tableSql;
    }

    public DataType[] getDataTypeList() {
        return this.dataTypeList;
    }

    public HashMap<String, DataType> getDataTypeMap() {
        return this.dataTypeMap;
    }


    private TableSchema createTableScheme() {
        TableSchema.Builder builder = TableSchema.builder();
        for (int i = 0; i < this.fieldList.length; i++) {
            builder.field(this.fieldList[i], this.dataTypeList[i]);
        }

        return builder.build();
    }

    public String createTableSql() {

        StringBuilder filedBuilder = new StringBuilder(this.fieldList.length);

        for (int i = 0; i < this.fieldList.length; i++) {
            if (i < this.fieldList.length - 1) {
                filedBuilder.append(String.format("%s %s,", this.fieldList[i], this.dataTypeList[i].getLogicalType().asSerializableString()));
            } else {
                filedBuilder.append(String.format("%s %s", this.fieldList[i], this.dataTypeList[i].getLogicalType().asSerializableString()));
            }
        }
        // todo 主键

        // 设置hologres 连接属性
        StringBuilder holoBuilder = new StringBuilder();

        holoBuilder.append("'dbname' = '").append(props.getDataBase()).append("'")
                .append(",'tablename' = '").append(props.getTableName()).append("'")
                .append(",'username' = '").append(props.getUserName()).append("'")
                .append(",'password' = '").append(props.getPassword()).append("'")
                .append(",'endpoint' = '").append(props.getEndpoint()).append("'");

        if (props.isPartitionrouter()) {
            holoBuilder.append(",'partitionrouter' = 'true'");
        }

        if (props.getMutatetype() != null) {
            holoBuilder.append(",'mutatetype' = '").append(props.getMutatetype()).append("'");
        }

        if (props.isIgnorNullWhenUpdate()) {
            holoBuilder.append(",'ignorenullwhenupdate' = 'true'");
        }

        // todo,其他参数


        return String.format(props.getCreateTableSqlTamplate(), props.getTableName(), filedBuilder.toString(), holoBuilder.toString());

    }

    public HologresSinkFunction getHoloSinkFunc() {

        Configuration config = createConfiguration(this.props);

        HologresConnectionParam hologresConnectionParam = new HologresConnectionParam(config);

        TableSchema schema = getTableSchema();

        HologresTableSchema hologresTableSchema = HologresTableSchema.get(hologresConnectionParam);
        AbstractHologresWriter<RowData> hologresWriter;
        if (config.getBoolean(HologresConfigs.USE_RPC_MODE)) {
            hologresWriter =
                    HologresRpcWriter.createRowDataWriter(
                            hologresConnectionParam, schema, hologresTableSchema);
        } else {
            hologresWriter =
                    HologresJDBCWriter.createRowDataWriter(
                            hologresConnectionParam, schema, hologresTableSchema);
        }
        return new HologresSinkFunction(hologresConnectionParam, hologresWriter);
    }

}
