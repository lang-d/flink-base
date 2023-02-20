package base.flink.pojo;


import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

/**
 * pojo 的基本类,包含所有的公共字段和方法
 */
public class BasePojo implements Serializable {

    public static final long serialVersionUID = 1L;

    public String logic_id;
    public String normal_id;
    public String data_type;
    public LocalDateTime gmt_create;
    public Long event_time;

    public String trace_id;// 上游标记

    // 额外的一些参数
    public String extra;


    // 数据进入系统的时间,json_details 才会记录
    public LocalDateTime process_time;

    // 当业务需要新增字段,这个时候如果改原来的pojo可能会涉及到反序列化的问题
    // 可以把新的字段放里面,做兼容
    public HashMap<String,Object> fields;


    // 数据处理时间
    public LocalDateTime ana_time;

    public void setAnaTime() {
        this.ana_time = LocalDateTime.now();
    }


    /**
     * 将localDateTime 格式化为字符串
     * @param localDateTime 时间
     * @return 格式化字符串
     */
    public String timeToString(LocalDateTime localDateTime){
        return timeToString(localDateTime,"yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 将localDateTime 格式化为字符串
     * @param localDateTime 时间
     * @return 格式化字符串
     */
    public String timeToString(LocalDateTime localDateTime,String pattern){
        return localDateTime.format(DateTimeFormatter.ofPattern(pattern));
    }

    public String anaTimeToString(){
        return timeToString(ana_time);
    }

    public String gmtCreateToString(){
        return timeToString(gmt_create);
    }


    @Override
    public String toString() {
        return JSONObject.toJSONString(this);
    }
}
