package beibei.trident.function;

import java.util.HashMap;
import java.util.Map;

public class Province {


    public static Map<String, String> getProvMap() {
        Map<String, String> provMap = new HashMap<String, String>();
        provMap.put("1", "上海");
        provMap.put("2", "北京");
        provMap.put("3", "广州");
        provMap.put("4", "深圳");

        provMap.put("5", "武汉");
        provMap.put("6", "成都");
        provMap.put("7", "天津");
        provMap.put("8", "大连");
        return provMap;
    }

}
