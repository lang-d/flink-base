package base.flink.pojo;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 发送任务通道的实体类,携带任务信息给下游
 */
public class TaskMessagePojo {

    public String logic_id; // 任务需要的一个逻辑id

    public String normal_id; // 任务相关联的id

    public String task_name; // 任务名

    public String task_type; // 任务类型,采集,计算等

    public String extra; // 任务需要的额外的一些参数,封装成一个json给下游

    public String gmt_create; // 任务创建的时间

    public long event_time; // 任务创建的一个时间戳

    public Integer level; // 任务优先级

    public TaskMessagePojo() {
        event_time = System.currentTimeMillis();
        gmt_create = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(event_time));
    }

}
