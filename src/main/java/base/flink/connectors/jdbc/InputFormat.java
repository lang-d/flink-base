package base.flink.connectors.jdbc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.split.JdbcGenericParameterValuesProvider;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;

public class InputFormat {

    /**
     * cloumnFields 元素的顺序要和cloumnTypes对应
     */
    private String[] cloumnFields;

    private TypeInformation[] cloumnTypes;

    /**
     * 字段过多是用这个更方便
     */
    private HashMap<String, TypeInformation> cloumnMap;

    /**
     * 这个参数可以用来调整并发度
     * 第一个数组的个数可以用来控制并行个数
     * 如:
     * SQL:select aweme_id,product_id,_partition from dwd_product_aweme WHERE _partition = ?
     * Serializable[][] parameters = new String[][]{{"20210119"},{"20210118"}};
     * 这里的并发度是2,同时获取 _partition='20210119' 和 _partition='20210118' 的数据
     */
    private Serializable[][] parameters;

    private final static String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";

    private final static String POSTGRES_DRIVER_NAME = "org.postgresql.Driver";

    private final static String MYSQL_JDBC = "mysql";

    private final static String POSTGRES_JDBC = "postgres";

    private String username;

    private String password;

    private int port = 3306;

    private String host;

    private String dataBase;

    private String query;

    private Integer resultSetConcurrency;

    private Integer fetchSize;

    public InputFormat setResultSetConcurrency(Integer resultSetConcurrency) {
        this.resultSetConcurrency = resultSetConcurrency;
        return this;
    }

    public InputFormat setFetchSize(Integer fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public InputFormat setParameters(Serializable[][] parameters) {
        this.parameters = parameters;
        return this;
    }


    public InputFormat setHost(String host) {
        this.host = host;
        return this;
    }

    public InputFormat setDataBase(String dataBase) {
        this.dataBase = dataBase;
        return this;
    }


    public InputFormat setPort(int port) {
        this.port = port;
        return this;
    }

    public InputFormat setUsername(String username) {
        this.username = username;
        return this;
    }

    public InputFormat setPassword(String password) {
        this.password = password;
        return this;
    }

    public InputFormat setQuery(String query) {
        this.query = query;
        return this;
    }

    public InputFormat setCloumnFields(String[] cloumnFields) {
        this.cloumnFields = cloumnFields;
        return this;
    }

    public InputFormat setCloumnTypes(TypeInformation[] cloumnTypes) {
        this.cloumnTypes = cloumnTypes;
        return this;
    }

    public InputFormat setCloumnMap(HashMap<String, TypeInformation> cloumnMap) {
        this.cloumnMap = cloumnMap;
        return this;
    }

    public InputFormat loadProperties(String filePath) throws Exception {
        Properties props = new Properties();
        props.load(InputFormat.class.getClassLoader().getResourceAsStream(filePath));

        return loadProperties(props);
    }

    public InputFormat loadProperties(Properties props) {
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

    public void buildCloumnInfo() {
        if (this.cloumnMap != null) {
            this.cloumnFields = new String[this.cloumnMap.size()];
            this.cloumnTypes = new TypeInformation[this.cloumnMap.size()];
            int index = 0;
            for (String field : this.cloumnMap.keySet()) {
                this.cloumnFields[index] = field;
                this.cloumnTypes[index] = this.cloumnMap.get(field);

                index++;
            }
        }
    }

    public JdbcInputFormat finishMysql() {

        String dbUrl = String.format("jdbc:mysql://%s:%s/%s", this.host, this.port, this.dataBase);

        return this.doFinish(MYSQL_DRIVER_NAME, dbUrl);

    }

    public JdbcInputFormat finishPostgre() {

        String dbUrl = String.format("jdbc:postgresql://%s:%s/%s", this.host, this.port, this.dataBase);

        return this.doFinish(POSTGRES_DRIVER_NAME, dbUrl);

    }

    public JdbcInputFormat doFinish(String driverName, String dbUrl) {
        this.buildCloumnInfo();

        RowTypeInfo rowTypeInfo = null;

        if (this.cloumnTypes != null && this.cloumnFields != null) {
            rowTypeInfo = new RowTypeInfo(this.cloumnTypes, this.cloumnFields);
        }

        if (this.cloumnTypes != null && this.cloumnFields == null) {
            rowTypeInfo = new RowTypeInfo(this.cloumnTypes);
        }

        JdbcInputFormat.JdbcInputFormatBuilder format = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername(driverName)
                .setDBUrl(dbUrl)
                .setPassword(this.password)
                .setUsername(this.username)
                .setQuery(this.query)
                .setRowTypeInfo(rowTypeInfo);

        if (this.parameters != null) {
            format.setParametersProvider(new JdbcGenericParameterValuesProvider(parameters));
        }

        if (this.resultSetConcurrency != null) {
            format.setResultSetConcurrency(this.resultSetConcurrency);
        }

        if (this.fetchSize != null) {
            format.setFetchSize(this.fetchSize);
        }

        return format.finish();
    }

    public JdbcInputFormat finish(String jdbcType) throws Exception {

        switch (jdbcType) {
            case MYSQL_JDBC:
                return finishMysql();
            case POSTGRES_JDBC:
                return finishPostgre();
            default:
                throw new Exception(String.format("not support %s jdbc type", jdbcType));
        }
    }


}
