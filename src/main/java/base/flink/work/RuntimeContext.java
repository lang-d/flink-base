package base.flink.work;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RuntimeContext {
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private StatementSet statementSet;

    private WorkFailHander failHander;

    public StreamExecutionEnvironment getStreamEnv(){
        if(this.env == null){
            this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        }

        return this.env;
    }

    public StreamTableEnvironment getStreamTableEnv(){
        if(this.tEnv == null){
            this.tEnv = StreamTableEnvironment.create(getStreamEnv(), EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build());
        }

        return this.tEnv;
    }

    public StatementSet getStatementSet(){
        if(this.statementSet == null){
            this.statementSet = getStreamTableEnv().createStatementSet();
        }
        return this.statementSet;
    }

    public void setFailHander(WorkFailHander failHander){
        this.failHander = failHander;
    }

    public WorkFailHander getFailHander(){return this.failHander;}


}
