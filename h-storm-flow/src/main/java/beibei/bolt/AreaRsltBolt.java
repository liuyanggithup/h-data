package beibei.bolt;

import beibei.dao.HBaseDAO;
import beibei.dao.imp.HBaseDAOImp;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;


public class AreaRsltBolt implements IBasicBolt {

    Map<String, Double> countsMap = null;
    HBaseDAO dao = null;
    long beginTime = System.currentTimeMillis();
    long endTime = 0L;

    @Override
    public void cleanup() {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String date_areaid = input.getString(0);
        double order_amt = input.getDouble(1);
        countsMap.put(date_areaid, order_amt);

        endTime = System.currentTimeMillis();
        /**
         * 5s更新一次HBase
         */
        if (true||endTime - beginTime >= 5 * 1000) {
            for (String key : countsMap.keySet()) {
                // put into hbase
                // 2014-05-05_1,amt
                dao.insert("area_order", key, "cf", "order_amt", countsMap.get(key) + "");
                System.err.println("rsltBolt put hbase: key=" + key + "; order_amt=" + countsMap.get(key));
            }
            beginTime = System.currentTimeMillis();
        }

    }

    /**
     * 初始化操作
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        dao = new HBaseDAOImp();
        countsMap = new HashMap<String, Double>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
