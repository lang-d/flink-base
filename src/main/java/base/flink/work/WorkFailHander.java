package base.flink.work;

/**
 * 作业失败后的异常处理
 */
public interface WorkFailHander {

    public void dealFail(Exception e);
}
