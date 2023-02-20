package base.flink.connectors.jdbc;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Properties;

public class JDBCSinkFormat<T> {

    private final static String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";

    private final static String POSTGRES_DRIVER_NAME = "org.postgresql.Driver";

    private final static String MYSQL_JDBC = "mysql";

    private final static String POSTGRES_JDBC = "postgres";

    private String jdbcType = MYSQL_JDBC;

    private String username;

    private String password;

    private int port = 3306;

    private String host;

    private String dataBase;

    private String sql;

    private HashMap<String,String> params;

    // 设置写入的参数
    private JdbcStatementBuilder<T> statementBuilder;

    // 更自由的去控制写入
    private JdbcExecutionOptions executionOptions;


    // 添加额外的链接参数
    public JDBCSinkFormat<T> addParams(String key,String value) {
        if (this.params == null){
            this.params = new HashMap<>();
        }

        this.params.put(key,value);

        return this;
    }

    public JDBCSinkFormat<T> setExecutionOptions(JdbcExecutionOptions executionOptions) {
        this.executionOptions = executionOptions;
        return this;
    }

    public JDBCSinkFormat<T> setStatementBuilder(JdbcStatementBuilder<T> statementBuilder) {
        this.statementBuilder = statementBuilder;
        return this;
    }

    public JDBCSinkFormat<T> setHost(String host) {
        this.host = host;
        return this;
    }

    public JDBCSinkFormat<T> setUsername(String username) {
        this.username = username;
        return this;
    }

    public JDBCSinkFormat<T> setPassword(String password) {
        this.password = password;
        return this;
    }

    public JDBCSinkFormat<T> setDataBase(String dataBase) {
        this.dataBase = dataBase;
        return this;
    }

    public JDBCSinkFormat<T> setPort(int port) {
        this.port = port;
        return this;
    }

    public JDBCSinkFormat<T> setSql(String sql) {
        this.sql = sql;
        return this;
    }

    public JDBCSinkFormat<T> setJdbcType(String jdbcType) {
        this.jdbcType = jdbcType;
        return this;
    }


    public JDBCSinkFormat<T> loadProperties(String filePath) throws Exception {
        Properties props = new Properties();
        props.load(JDBCSinkFormat.class.getClassLoader().getResourceAsStream(filePath));

        return loadProperties(props);
    }

    public JDBCSinkFormat<T> loadProperties(Properties props) {
        if (props.getProperty("host") != null) {
            this.host = props.getProperty("host");
        }
        if (props.getProperty("username") != null) {
            this.username = props.getProperty("username");
        }
        if (props.getProperty("password") != null) {
            this.password = props.getProperty("password");
        }
        if (props.getProperty("database") != null) {
            this.dataBase = props.getProperty("database");
        }

        if (props.getProperty("port") != null) {
            this.port = Integer.parseInt(props.getProperty("port"));
        }

        return this;
    }

    private String getUrl() {
        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append("jdbc:").
                append(this.jdbcType).
                append("://").
                append(this.host).
                append(":").
                append(this.port).
                append("/").
                append(this.dataBase);
        if (this.params!=null){
            urlBuilder.append("?");
            this.params.forEach((k,v)->{
                urlBuilder.append(k).append("=").append(v).append("&");
            });
        }

        return urlBuilder.toString();
    }

    private String getDriverName() throws Exception {
        Preconditions.checkNotNull(this.jdbcType);
        if (this.jdbcType.equals(MYSQL_JDBC)) {
            return MYSQL_DRIVER_NAME;
        }

        if (this.jdbcType.equals(POSTGRES_JDBC)) {
            return POSTGRES_DRIVER_NAME;
        }

        throw new Exception("not support jdbc type:" + jdbcType);
    }


    private JdbcConnectionOptions createConnectionOptions() throws Exception {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(this.getUrl())
                .withDriverName(getDriverName())
                .withUsername(this.username)
                .withPassword(this.password)
                .build();
    }

    /**
     * 核心方法,调用这个就可以了
     * @return
     * @throws Exception
     */
    public SinkFunction<T> getSinkFunc() throws Exception {
        Preconditions.checkNotNull(this.sql);
        Preconditions.checkNotNull(this.statementBuilder);

        if (this.executionOptions == null) {
            return JdbcSink.sink(this.sql, this.statementBuilder, createConnectionOptions());
        }

        return JdbcSink.sink(this.sql, this.statementBuilder, this.executionOptions, createConnectionOptions());

    }

}
