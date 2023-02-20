package base.flink.work;

public abstract class BaseWork implements Work {

    private RuntimeContext runtimeContext;

    public RuntimeContext getRuntimeContext() {
        if (this.runtimeContext == null) {
            this.runtimeContext = new RuntimeContext();
        }
        return this.runtimeContext;
    }

    public void init() {
        getRuntimeContext().setFailHander(new PrintFailHander());
    }

    @Override
    public void excute() {
        try {
            this.init();
            this.work();
        } catch (Exception e) {
            getRuntimeContext().getFailHander().dealFail(e);
        } finally {
            done();
        }
    }

    // 子类在这里写处理逻辑
    public abstract void work() throws Exception;

    public void done() {
    }
}
