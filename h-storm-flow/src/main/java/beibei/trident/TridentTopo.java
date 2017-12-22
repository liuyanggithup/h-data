package beibei.trident;


import beibei.trident.function.OrderAmtSplit;
import beibei.trident.function.OrderNumSplit;
import beibei.trident.function.Split;
import beibei.trident.function.SplitBy;
import hbase.state.HBaseAggregateState;
import hbase.state.TridentConfig;
import kafka.productor.KafkaProperties;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class TridentTopo {

	public static StormTopology builder(LocalDRPC drpc)
	{
		TridentConfig tridentConfig = new TridentConfig("state");
		StateFactory state = HBaseAggregateState.transactional(tridentConfig);
		BrokerHosts zkHosts = new ZkHosts(KafkaProperties.zkConnect);
		String topic = "track";
		TridentKafkaConfig config = new TridentKafkaConfig(zkHosts, topic);
		config.scheme = new SchemeAsMultiScheme(new StringScheme());
		config.fetchSizeBytes = 100 ;//batch size
		
		TransactionalTridentKafkaSpout spout  = new TransactionalTridentKafkaSpout(config) ;
		
		TridentTopology topology = new TridentTopology() ;
		//销售额
		TridentState amtState = topology.newStream("spout", spout)
		.parallelismHint(3)
		.each(new Fields(StringScheme.STRING_SCHEME_KEY),new OrderAmtSplit("\\t"), new Fields("order_id","order_amt","create_date","province_id","cf"))
		.shuffle()
		.groupBy(new Fields("create_date","cf","province_id"))
		.persistentAggregate(state, new Fields("order_amt"), new Sum(), new Fields("sum_amt"));
//		.persistentAggregate(new MemoryMapState.Factory(), new Fields("order_amt"), new Sum(), new Fields("sum_amt"));
		
		topology.newDRPCStream("getOrderAmt", drpc)
		.each(new Fields("args"), new Split(" "), new Fields("arg"))
		.each(new Fields("arg"), new SplitBy("\\:"), new Fields("create_date","cf","province_id"))
		.groupBy(new Fields("create_date","cf","province_id"))
		.stateQuery(amtState, new Fields("create_date","cf","province_id"), new MapGet(), new Fields("sum_amt"))
//		.applyAssembly(new FirstN(5, "sum_amt", true))
		;
		
		//订单数
		TridentState orderState = topology.newStream("orderSpout", spout)
		.parallelismHint(3)
		.each(new Fields(StringScheme.STRING_SCHEME_KEY),new OrderNumSplit("\\t"), new Fields("order_id","order_amt","create_date","province_id","cf"))
		.shuffle()
		.groupBy(new Fields("create_date","cf","province_id"))
		.persistentAggregate(state, new Fields("order_id"), new Count(), new Fields("order_num"));
//		.persistentAggregate(new MemoryMapState.Factory(), new Fields("order_id"), new Count(), new Fields("order_num"));
		
		topology.newDRPCStream("getOrderNum", drpc)
		.each(new Fields("args"), new Split(" "), new Fields("arg"))
		.each(new Fields("arg"), new SplitBy("\\:"), new Fields("create_date","cf","province_id"))
		.groupBy(new Fields("create_date","cf","province_id"))
		.stateQuery(orderState, new Fields("create_date","cf","province_id"), new MapGet(), new Fields("order_num"))
//		.applyAssembly(new FirstN(5, "order_num", true))
		;
		return topology.build() ;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LocalDRPC drpc = new LocalDRPC();
		Config conf = new Config() ;
		conf.setDebug(false);
		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder(null));
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder(drpc));
		}
		
		while (true) {
			System.err.println("销售额："+drpc.execute("getOrderAmt", "2014-08-19:cf:amt_1 2014-08-19:cf:amt_2")) ;
			System.err.println("订单数："+drpc.execute("getOrderNum", "2014-08-19:cf:orderNum_1 2014-08-19:cf:orderNum_2")) ;
			Utils.sleep(5000);
		}
		/**
		 * [["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:1","2014-08-19","1",821.9000000000001],
		 * ["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:2","2014-08-19","2",631.3000000000001],
		 * ["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:3","2014-08-19","3",240.7],
		 * ["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:4","2014-08-19","4",340.4],
		 * ["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:5","2014-08-19","5",460.8]]
		 */
		
		
	}

}
