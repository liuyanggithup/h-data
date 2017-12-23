package beibei.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import tools.DateFmt;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;



public class UVBolt1 implements IRichBolt {

	Map<String, Long> map = new HashMap<String, Long>() ;
	Set<String> hasEmittedSet = new HashSet<String>() ;
	private static final long serialVersionUID = 1L;

	OutputCollector collector = null;
	String today = null;
	
	@Override
	public void cleanup() {
	}

	@Override
	public void execute(Tuple input) {
		try {
			if (input != null) {
				String date = input.getString(0) ;
				if (today.compareTo(date) < 0 ) {
					//跨天处理
					today = date ;
					map.clear() ;
					hasEmittedSet.clear() ;
				}
				
				String session_id = input.getString(1) ;
				String key = date+"_"+session_id ;
				if (hasEmittedSet.contains(key)) {
					throw new Exception("this tuple has emitted ...") ;
				}
				
				Long pv = map.get(key) ;
				if (pv == null) {
					pv = 0L ;
				}
				pv ++ ;
				map.put(key, pv) ;
				if (pv >= 2) {
					collector.emit(new Values(key)) ;
					hasEmittedSet.add(key) ;
				}
				collector.ack(input);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector ;
		today = DateFmt.getCountDate(null, DateFmt.date_short) ;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date_SessionId"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
