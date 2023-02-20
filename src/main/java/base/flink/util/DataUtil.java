package base.flink.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class DataUtil {
    public static StringData costString(String value) {
        if (value != null) {
            return StringData.fromString(value);
        }
        return null;
    }

    public static StringData costJsonString(Object value) {
        if (value != null) {
            return StringData.fromString(JSONObject.toJSONString(value));
        }
        return null;
    }

    public static TimestampData costTimeStamp(LocalDateTime value) {
        if (value != null) {
            return TimestampData.fromLocalDateTime(value);
        }

        return null;
    }

    public static GenericArrayData costStringArray(List<String> arr) {
        if (arr == null) {
            return null;
        }
        ArrayList<StringData> arrayList = new ArrayList<StringData>(arr.size());
        for (String item : arr) {
            arrayList.add(costString(item));
        }
        return new GenericArrayData(arrayList.toArray());
    }

    public static GenericArrayData costIntegerArray(List<Integer> arr) {
        if (arr == null) {
            return null;
        }
        return new GenericArrayData(arr.toArray());
    }

    public static GenericArrayData costLongArray(List<Long> arr) {
        if (arr == null) {
            return null;
        }
        return new GenericArrayData(arr.toArray());
    }

    public static GenericArrayData costDoubleArray(List<Double> arr) {
        if (arr == null) {
            return null;
        }
        return new GenericArrayData(arr.toArray());
    }

    public static GenericArrayData costFloatArray(List<Float> arr) {
        if (arr == null) {
            return null;
        }
        return new GenericArrayData(arr.toArray());
    }

    public static GenericArrayData costTimestampArray(List<LocalDateTime> arr) {
        if (arr == null) {
            return null;
        }
        ArrayList<TimestampData> arrayList = new ArrayList<TimestampData>(arr.size());
        for (LocalDateTime item : arr) {
            arrayList.add(costTimeStamp(item));
        }
        return new GenericArrayData(arrayList.toArray());
    }

}
