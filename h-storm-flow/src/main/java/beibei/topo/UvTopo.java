package beibei.topo;

import beibei.bolt.LogFmtBolt;
import beibei.bolt.UVBolt1;
import beibei.bolt.UVRsltBolt;
import beibei.spout.LogSpout;
import kafka.productor.KafkaProperties;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class UvTopo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new LogSpout(KafkaProperties.Log_topic), 3);
		builder.setBolt("fmtBolt", new LogFmtBolt(), 3).shuffleGrouping("spout");
		builder.setBolt("UVBolt1", new UVBolt1() , 5).fieldsGrouping("fmtBolt", new Fields("date","session_id")) ;
		
		builder.setBolt("rsltBolt", new UVRsltBolt(), 1).shuffleGrouping("UVBolt1");
		
		Config conf = new Config() ;
		conf.setDebug(false);
		conf.setNumWorkers(10);
		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder.createTopology());
		}
		
		
	}

}
