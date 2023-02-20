package base.flink.pojo;


import com.alibaba.fastjson.JSONObject;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * 从采集过来的原始数据
 */
public class JsonDetailsPojo extends BasePojo {

    public String json_details;

    public String _partition;

    public void setProcessTime() {
        this.process_time = LocalDateTime.now();

        setAnaTime();

    }

    public void setPartition() {
        this._partition = new SimpleDateFormat("yyyyMMdd").format(new Date(this.event_time));
    }

    @Override
    public String toString() {
        return JSONObject.toJSONString(this);
    }
}
