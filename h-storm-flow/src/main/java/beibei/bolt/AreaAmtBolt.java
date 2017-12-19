package beibei.bolt;

import beibei.dao.HBaseDAO;
import beibei.dao.imp.HBaseDAOImp;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import tools.DateFmt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class AreaAmtBolt implements IBasicBolt {

    String today = null;
    HBaseDAO dao = null;
    Map<String, Double> countsMap = null;

    @Override
    public void cleanup() {
        countsMap.clear();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if (tuple != null) {
            /**
             * 数据流格式area_id,order_amt,create_time
             */
            String area_id = tuple.getString(0);
            double order_amt = 0.0;
            try {
                order_amt = Double.parseDouble(tuple.getString(1));
            } catch (Exception e) {
                System.out.println(tuple.getString(1) + ":---------------------------------");
                e.printStackTrace();
            }

            String order_date = tuple.getStringByField("order_date");
            //如果不是today的日期，说明跨天
            if (!order_date.equals(today)) {
                //跨天处理，清空countsMap
                countsMap.clear();
            }
            //根据order_date + "_" + area_id为key从map中取值计算
            Double count = countsMap.get(order_date + "_" + area_id);
            if (count == null) {
                count = 0.0;
            }
            count += order_amt;
            //将计算的值写入map
            countsMap.put(order_date + "_" + area_id, count);
            System.err.println("areaAmtBolt:" + order_date + "_" + area_id + "=" + count);
            //发射数据格式为"date_area", "amt"
            collector.emit(new Values(order_date + "_" + area_id, count));
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        dao = new HBaseDAOImp();
        //取到当前日期
        today = DateFmt.getCountDate(null, DateFmt.date_short);
        //根据HBase里初始值进行初始化 countsMap
        countsMap = this.initMap(today, dao);
        for (String key : countsMap.keySet()) {
            System.err.println("key:" + key + "; value:" + countsMap.get(key));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //定义下一级流转的数据结构为"date_area", "amt"
        declarer.declare(new Fields("date_area", "amt"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public Map<String, Double> initMap(String rowKeyDate, HBaseDAO dao) {
        Map<String, Double> countsMap = new HashMap<String, Double>();
        List<Result> list = dao.getRows("area_order", rowKeyDate, new String[]{"order_amt"});

        for (Result rsResult : list) {
            String rowKey = new String(rsResult.getRow());
            for (KeyValue keyValue : rsResult.raw()) {
                if ("order_amt".equals(new String(keyValue.getQualifier()))) {
                    countsMap.put(rowKey, Double.parseDouble(new String(keyValue.getValue())));
                    break;
                }
            }
        }
        return countsMap;
    }

}
