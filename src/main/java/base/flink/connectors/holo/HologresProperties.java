package base.flink.connectors.holo;

import java.util.Properties;

/**
 * 用于设置数据sink进hologres 的属性
 */
public class HologresProperties {

    private String dataBase;

    private String tableName;

    private String userName;

    private String password;

    // 写入模式 保留首次写入,其他忽略
    public static final String INSERT_OR_IGNORE = "insertorignore";

    // 写入模式 替换已有
    public static final String INSERT_OR_REPLACE = "insertorreplace";

    // 写入模式 更新局部
    public static final String INSERT_OR_UPDATE = "insertorupdate";


    // 忽略null值的写入
    private boolean ignore_null_when_update ;

    private String endpoint;

    // 是否路由到分区表
    private boolean partitionrouter;

    // 写入模式
    private String mutatetype;

    // 是否自动创建建分区
    private boolean createparttable;

    // 	是否忽略撤回消息。
    private Boolean ignoredelete;

    // 是否使用rpc模式,默认为true
    private boolean useRpcMode = true;

    // jdbc连接池的大小
    private Integer connectionSize;

    // JDBC模式，Hologres Sink节点数据攒批的最大值。
    private Integer jdbcWriteBatchSize;

    // JDBC模式，Hologres Sink节点数据攒批写入Hologres的最长等待时间。
    private Long jdbcWriteFlushInterval;

    // 连接池名称。同一个TaskManager中，配置相同名称的连接池的表可以共享连接池。
    // 每个表默认使用自己的连接池。如果多个表设置相同的连接池，则这些使用相同连接池的表的connectorSize都需要相同
    private String connectionPoolName;

    // 	Hologres Sink节点数据攒批处理的最大字节数。
    // 默认值为2097152（2 * 1024 * 1024）字节，2 MB。
    //说明 jdbcWriteBatchSize和jdbcWriteFlushInterval之间为或的关系。如果同时设置了这两个参数，则满足其中一个，就进行写入结果数据。
    private Long jdbcWriteBatchByteSize;

    // Hologres表中not null且没有设置默认值的字段，是否允许写入null值。
    // 默认是不会去写的
    private boolean jdbcEnableDefaultForNotNullColumn;

    // 当连接故障时，写入和查询的重试次数。
    // 默认值为10。
    private Integer jdbcRetryCount;

    // 每次重试的固定等待时间。
    //实际重试的等待时间的计算公式为retrySleepInitMs+retry*retrySleepStepMs。
    // 默认值为1000毫秒。
    private Long jdbcRetrySleepInitMs;

    // 每次重试的累加等待时间。
    //实际重试的等待时间的计算公式为retrySleepInitMs+retry*retrySleepStepMs。
    // 默认值为5000毫秒。
    private Long jdbcRetrySleepStepMs;

    // JDBC连接的空闲时间。
    //超过这个空闲时间，连接就会断开释放掉。
    // 默认值为60000毫秒。
    private Long jdbcConnectionMaxIdleMs;

    // 本地缓存TableSchema信息的过期时间。
    // 默认值为60000毫秒。
    private Long jdbcMetaCacheTTL;

    // 如果Cache的剩余时间小于触发时间（jdbcMetaCacheTTL/jdbcMetaAutoRefreshFactor），则系统会自动刷新Cache。
    // 默认值为4。
    //Cache的剩余时间=Cache的过期时间 - Cache已经存活的时间。
    //Cache自动刷新后，则从0开始重新计算Cache的存活时间。
    private Integer jdbcMetaAutoRefreshFactor;

    // 建表生成模板
    private String createTableSqlTamplate = "CREATE TABLE %s (%s) with ('connector'='hologres',%s)";

    public HologresProperties loadProperties(String filePath) throws Exception {
        Properties props = new Properties();
        props.load(HologresProperties.class.getClassLoader().getResourceAsStream(filePath));

        return loadProperties(props);
    }

    public HologresProperties loadProperties(Properties props) {
        if (props.getProperty("endpoint") != null) {
            this.endpoint = props.getProperty("endpoint");
        }
        if (props.getProperty("username") != null) {
            this.userName = props.getProperty("username");
        }
        if (props.getProperty("password") != null) {
            this.password = props.getProperty("password");
        }
        if (props.getProperty("database") != null) {
            this.dataBase = props.getProperty("database");
        }

        return this;
    }



    public HologresProperties setDataBase(String dataBase) {
        this.dataBase = dataBase;
        return this;
    }

    public HologresProperties setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public HologresProperties setUserName(String userName) {
        this.userName = userName;
        return this;
    }

    public HologresProperties setPassword(String password) {
        this.password = password;
        return this;
    }

    public HologresProperties setEndpoint(String endpoint) {
        this.endpoint = endpoint;
        return this;
    }

    public HologresProperties setPartitionrouter(boolean partitionrouter) {
        this.partitionrouter = partitionrouter;
        return this;
    }

    public HologresProperties setMutatetype(String mutatetype) {
        this.mutatetype = mutatetype;
        return this;
    }

    public HologresProperties setIgnorNullWhenUpdate() {
        this.ignore_null_when_update = true;
        return this;
    }


    public boolean isCreateparttable() {
        return createparttable;
    }

    public HologresProperties setCreateparttable(boolean createparttable) {
        this.createparttable = createparttable;
        return this;
    }

    public boolean isUseRpcMode() {
        return useRpcMode;
    }

    public HologresProperties setUseRpcMode(boolean useRpcMode) {
        this.useRpcMode = useRpcMode;
        return this;
    }

    public Integer getConnectionSize() {
        return connectionSize;
    }

    public HologresProperties setConnectionSize(Integer connectionSize) {
        this.connectionSize = connectionSize ;
        return this;
    }

    public Integer getJdbcWriteBatchSize() {
        return jdbcWriteBatchSize;
    }

    public HologresProperties setJdbcWriteBatchSize(Integer jdbcWriteBatchSize) {
        this.jdbcWriteBatchSize = jdbcWriteBatchSize;
        return this;
    }

    public Long getJdbcWriteFlushInterval() {
        return jdbcWriteFlushInterval;
    }

    public HologresProperties setJdbcWriteFlushInterval(Long jdbcWriteFlushInterval) {
        this.jdbcWriteFlushInterval = jdbcWriteFlushInterval;
        return this;
    }

    public String getDataBase() {
        return dataBase;
    }

    public String getTableName() {
        return tableName;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public boolean isPartitionrouter() {
        return partitionrouter;
    }

    public String getMutatetype() {
        return mutatetype;
    }

    public String getCreateTableSqlTamplate() {
        return createTableSqlTamplate;
    }

    public boolean isIgnorNullWhenUpdate() {
        return this.ignore_null_when_update;
    }

    public String getConnectionPoolName() {
        return connectionPoolName;
    }

    public HologresProperties setConnectionPoolName(String connectionPoolName) {
        this.connectionPoolName = connectionPoolName;
        return this;
    }

    public Long getJdbcWriteBatchByteSize() {
        return jdbcWriteBatchByteSize;
    }

    public HologresProperties setJdbcWriteBatchByteSize(Long jdbcWriteBatchByteSize) {
        this.jdbcWriteBatchByteSize = jdbcWriteBatchByteSize;
        return this;
    }

    public boolean isJdbcEnableDefaultForNotNullColumn() {
        return jdbcEnableDefaultForNotNullColumn;
    }

    public HologresProperties setJdbcEnableDefaultForNotNullColumn(boolean jdbcEnableDefaultForNotNullColumn) {
        this.jdbcEnableDefaultForNotNullColumn = jdbcEnableDefaultForNotNullColumn;
        return this;
    }

    public Integer getJdbcRetryCount() {
        return jdbcRetryCount;
    }

    public HologresProperties setJdbcRetryCount(Integer jdbcRetryCount) {
        this.jdbcRetryCount = jdbcRetryCount;
        return this;
    }

    public Long getJdbcRetrySleepInitMs() {
        return jdbcRetrySleepInitMs;
    }

    public HologresProperties setJdbcRetrySleepInitMs(Long jdbcRetrySleepInitMs) {
        this.jdbcRetrySleepInitMs = jdbcRetrySleepInitMs;
        return this;
    }

    public Long getJdbcRetrySleepStepMs() {
        return jdbcRetrySleepStepMs;
    }

    public HologresProperties setJdbcRetrySleepStepMs(Long jdbcRetrySleepStepMs) {
        this.jdbcRetrySleepStepMs = jdbcRetrySleepStepMs;
        return this;
    }

    public Long getJdbcConnectionMaxIdleMs() {
        return jdbcConnectionMaxIdleMs;
    }

    public HologresProperties setJdbcConnectionMaxIdleMs(Long jdbcConnectionMaxIdleMs) {
        this.jdbcConnectionMaxIdleMs = jdbcConnectionMaxIdleMs;
        return this;
    }

    public Long getJdbcMetaCacheTTL() {
        return jdbcMetaCacheTTL;
    }

    public HologresProperties setJdbcMetaCacheTTL(Long jdbcMetaCacheTTL) {
        this.jdbcMetaCacheTTL = jdbcMetaCacheTTL;
        return this;
    }

    public Integer getJdbcMetaAutoRefreshFactor() {
        return jdbcMetaAutoRefreshFactor;
    }

    public HologresProperties setJdbcMetaAutoRefreshFactor(Integer jdbcMetaAutoRefreshFactor) {
        this.jdbcMetaAutoRefreshFactor = jdbcMetaAutoRefreshFactor;
        return this;
    }

    public Boolean getIgnoredelete() {
        return ignoredelete;
    }

    public HologresProperties setIgnoredelete(Boolean ignoredelete) {
        this.ignoredelete = ignoredelete;
        return this;
    }

    @Override
    public String toString() {
        return "{" +
                "dataBase='" + dataBase + '\'' +
                ", tableName='" + tableName + '\'' +
                ", userName='" + userName + '\'' +
                ", password='" + password + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", partitionrouter=" + partitionrouter +
                ", mutatetype='" + mutatetype + '\'' +
                ", createTableSqlTamplate='" + createTableSqlTamplate + '\'' +
                '}';
    }
}
