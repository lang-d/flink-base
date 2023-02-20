package base.flink.connectors.es;

import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Preconditions;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

public class EsConnector<T> implements Serializable {
    private String host;
    private int port = 9200;
    private String username;
    private String password;

    private long bulkFlushInterval = 5L*1000L*60; // 5min
    private int bulkFlushMaxActions = 1000;
    private int bulkBackoffRetries = 3;

    private ActionRequestFailureHandler failureHandler;

    private ElasticsearchSinkFunction<T> sinkFunction;

    private static final long serialVersionUID = 1L;


    public EsConnector(){}

    public EsConnector(String host,int port,String username,String password){
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public EsConnector<T> setHost(String host){
        this.host = host;
        return this;
    }

    public EsConnector<T> setPort(int port){
        this.port = port;
        return this;
    }

    public EsConnector<T> setUsername(String username){
        this.username = username;
        return this;
    }

    public EsConnector<T> setPassword(String password){
        this.password = password;
        return this;
    }

    public EsConnector<T> setBulkFlushInterval(long bulkFlushInterval){
        this.bulkFlushInterval  = bulkFlushInterval;
        return this;
    }

    public EsConnector<T> setBulkFlushMaxActions(int bulkFlushMaxActions){
        this.bulkFlushMaxActions = bulkFlushMaxActions;
        return this;
    }

    public EsConnector<T> setBulkBackoffRetries(int bulkBackoffRetries){
        this.bulkBackoffRetries = bulkBackoffRetries;
        return this;
    }

    public EsConnector<T> setFailureHandler(ActionRequestFailureHandler failureHandler){
        this.failureHandler = failureHandler;
        return this;
    }

    public EsConnector<T> setSinkFunction(ElasticsearchSinkFunction<T> sinkFunction){
        this.sinkFunction = sinkFunction;
        return this;
    }

    public EsConnector<T> loadProperties(String filePath) throws Exception {
        Properties props = new Properties();
        props.load(EsConnector.class.getClassLoader().getResourceAsStream(filePath));

        return loadProperties(props);

    }

    public EsConnector<T> loadProperties(Properties props) throws Exception {
        if (props.getProperty("host") == null) {
            throw new Exception("no host");
        }
        if (props.getProperty("port") == null) {
            throw new Exception("no port");
        }
        if (props.getProperty("password") == null) {
            throw new Exception("no password");
        }
        if (props.getProperty("username") == null) {
            throw new Exception("username");
        }

        this.host = props.getProperty("host");
        this.port = Integer.parseInt(props.getProperty("port"));
        this.password = props.getProperty("password");
        this.username = props.getProperty("username");

        return this;
    }

    public ElasticsearchSink<T> buildSink(){
        Preconditions.checkNotNull(this.host);
        Preconditions.checkNotNull(this.username);
        Preconditions.checkNotNull(this.password);
        Preconditions.checkNotNull(this.sinkFunction);

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(this.host, this.port, "http"));
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<T>(
                httpHosts,
                sinkFunction
        );

        esSinkBuilder.setRestClientFactory(ele->{
            Header[] headers = new Header[]{new BasicHeader("Authorization",String.format("Basic %s", Base64.getEncoder().
                    encodeToString((this.username+":"+this.password).getBytes(StandardCharsets.UTF_8))))};
            ele.setDefaultHeaders(headers);
        });

        esSinkBuilder.setBulkFlushInterval(this.bulkFlushInterval);
        esSinkBuilder.setBulkFlushBackoffRetries(this.bulkBackoffRetries);
        esSinkBuilder.setBulkFlushMaxActions(this.bulkFlushMaxActions);

        if (this.failureHandler!=null){
            esSinkBuilder.setFailureHandler(failureHandler);
        }

        return esSinkBuilder.build();


    }


}
