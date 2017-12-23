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


public class LogFmtBolt implements IBasicBolt {

    @Override
    public void cleanup() {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String logString = input.getString(0);
        try {
            if (input != null) {
                String arr[] = logString.split("\\t");
                collector.emit(new Values(DateFmt.getCountDate(arr[2], DateFmt.date_short), arr[1]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("date", "session_id"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
