package base.flink.work;

public class IgnoreFailHander implements WorkFailHander {
    @Override
    public void dealFail(Exception e) {
        // no thing to do
    }
}
