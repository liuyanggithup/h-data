package beibei.topo;

import beibei.bolt.AreaAmtBolt;
import beibei.bolt.AreaFilterBolt;
import beibei.bolt.AreaRsltBolt;
import beibei.spout.OrderBaseSpout;
import kafka.productor.KafkaProperties;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class AreaAmtTopo {

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new OrderBaseSpout(KafkaProperties.Order_topic), 5);
		//随机分组
		builder.setBolt("filter", new AreaFilterBolt() , 5).shuffleGrouping("spout") ;
		//按字段分组
		builder.setBolt("areaBolt", new AreaAmtBolt() , 2).fieldsGrouping("filter", new Fields("area_id")) ;
		//统一处理
		builder.setBolt("rsltBolt", new AreaRsltBolt(), 1).shuffleGrouping("areaBolt");



		Config conf = new Config() ;
		conf.setDebug(true);

		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("topology", conf, builder.createTopology());
		}
		
		
	}

}
