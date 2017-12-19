package beibei.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import tools.DateFmt;

import java.util.Map;


public class AreaFilterBolt implements IBasicBolt {


    @Override
    public void cleanup() {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {

        //取到传入的数据
        String order = tuple.getString(0);
        if (order != null) {
            //将数据以空格拆分，数据格式为order_id,order_amt,create_time,area_id
            String orderArr[] = order.split("\\t");
            //发射数据为"area_id", "order_amt", "order_date"交给下一级bolt处理
            collector.emit(new Values(orderArr[3], orderArr[1], DateFmt.getCountDate(orderArr[2], DateFmt.date_short)));
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //定义下一级数据流的字段
        declarer.declare(new Fields("area_id", "order_amt", "order_date"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
