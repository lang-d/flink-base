package base.flink.work;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PrintFailHander implements WorkFailHander {

    private Logger LOG = LoggerFactory.getLogger(PrintFailHander.class);

    @Override
    public void dealFail(Exception e) {
        LOG.error("",e);
    }
}
