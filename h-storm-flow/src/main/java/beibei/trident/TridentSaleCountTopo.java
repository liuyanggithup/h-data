package beibei.trident;


import beibei.trident.function.OrderSplit;
import beibei.trident.function.Split;
import beibei.trident.function.SplitBy;
import kafka.productor.KafkaProperties;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class TridentSaleCountTopo {


    public static void main(String[] args) {

        BrokerHosts zkHosts = new ZkHosts(KafkaProperties.zkConnect);
        TridentKafkaConfig config = new TridentKafkaConfig(zkHosts, KafkaProperties.topic);
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        //旧版本设置config.forceFromStart=true
        config.ignoreZkOffsets=true;
        config.fetchSizeBytes = 100;
        TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(config);
        TridentTopology topology = new TridentTopology();
        LocalDRPC drpc = new LocalDRPC() ;

        //销售额
        TridentState amtState = topology.newStream("spout", spout)
                .parallelismHint(1)
                .each(new Fields(StringScheme.STRING_SCHEME_KEY),new OrderSplit("\\t"), new Fields("order_id","order_amt","create_date","province_id"))
                .groupBy(new Fields("create_date","province_id"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("order_amt"), new Sum(), new Fields("sum_amt"));

        topology.newDRPCStream("getOrderAmt", drpc)
                .each(new Fields("args"), new Split(" "), new Fields("arg"))
                .each(new Fields("arg"), new SplitBy("\\:"), new Fields("create_date","province_id"))
                .groupBy(new Fields("create_date","province_id"))
                .stateQuery(amtState, new Fields("create_date","province_id"), new MapGet(), new Fields("sum_amt"));

        Config conf = new Config() ;
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster() ;
        cluster.submitTopology("myTopo", conf, topology.build());

        while (true) {
            String getOrderAmt = drpc.execute("getOrderAmt", "2017-12-21:1 2017-12-21:2 2017-12-21:3 2017-12-21:4 2017-12-21:5");
            System.err.println("***###drpc(getOrderAmt)result["+getOrderAmt+"]###***") ;
            Utils.sleep(5000);
        }




    }


}
