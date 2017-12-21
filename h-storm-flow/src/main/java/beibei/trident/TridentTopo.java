package beibei.trident;


import beibei.trident.function.Print;
import kafka.productor.KafkaProperties;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.daemon.drpc;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class TridentTopo {


    public static void main(String[] args) {

        BrokerHosts zkHosts = new ZkHosts(KafkaProperties.zkConnect);
        String topic = "track";
        TridentKafkaConfig config = new TridentKafkaConfig(zkHosts, topic);
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        //batch size
        config.fetchSizeBytes = 100;
        TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(config);
        TridentTopology topology = new TridentTopology();
        topology.newStream("spout", spout).parallelismHint(1).each(new Fields(StringScheme.STRING_SCHEME_KEY), new Print(), new Fields("msg"));

//        //销售额
//        TridentState amtState = topology.newStream("spout", spout)
//                .parallelismHint(1)
//                .each(new Fields(StringScheme.STRING_SCHEME_KEY), new OrderSplit("\\t"), new Fields("order_id", "order_amt", "create_date", "province_id"))
//                .groupBy(new Fields("create_date", "province_id"))
//                .persistentAggregate(new MemoryMapState.Factory(), new Fields("order_amt"), new Sum(), new Fields("sum_amt"));
//
//        topology.newDRPCStream("getOrderAmt", drpc)
//                .each(new Fields("args"), new Split(" "), new Fields("arg"))
//                .each(new Fields("arg"), new SplitBy("\\:"), new Fields("create_date", "province_id"))
//                .groupBy(new Fields("create_date", "province_id"))
//                .stateQuery(amtState, new Fields("create_date", "province_id"), new MapGet(), new Fields("sum_amt"));

        Config conf = new Config();
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("myTopo", conf, topology.build());


    }


}
