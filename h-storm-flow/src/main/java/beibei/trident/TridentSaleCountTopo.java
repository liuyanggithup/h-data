package beibei.trident;


import beibei.trident.function.OrderSplit;
import beibei.trident.function.Split;
import beibei.trident.function.SplitBy;
import kafka.api.OffsetRequest;
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
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import tools.DateFmt;

public class TridentSaleCountTopo {


    public static void main(String[] args) {

        BrokerHosts zkHosts = new ZkHosts(KafkaProperties.zkConnect);
        TridentKafkaConfig config = new TridentKafkaConfig(zkHosts, KafkaProperties.topic);
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        //batch size
        config.fetchSizeBytes = 100;
        //旧版本设置config.forceFromStart=false
        //Todo 版本临时解决方案，后期研究
        config.startOffsetTime = OffsetRequest.LatestTime();
        TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(config);


        TridentTopology topology = new TridentTopology();
        LocalDRPC drpc = new LocalDRPC();

        //销售额
        TridentState amtState = topology.newStream("spout", spout)
                .parallelismHint(3)
                .each(new Fields(StringScheme.STRING_SCHEME_KEY), new OrderSplit("\\t"), new Fields("order_id", "order_amt", "create_date", "province_id"))
                .shuffle()
                .groupBy(new Fields("create_date", "province_id"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("order_amt"), new Sum(), new Fields("sum_amt"));

        topology.newDRPCStream("getOrderAmt", drpc).parallelismHint(1)
                .each(new Fields("args"), new Split(" "), new Fields("arg"))
                .each(new Fields("arg"), new SplitBy("\\:"), new Fields("create_date", "province_id"))
                .groupBy(new Fields("create_date", "province_id"))
                .stateQuery(amtState, new Fields("create_date", "province_id"), new MapGet(), new Fields("sum_amt"))
        ;

        //订单数
        TridentState orderState = topology.newStream("orderSpout", spout)
                .parallelismHint(3)
                .each(new Fields(StringScheme.STRING_SCHEME_KEY), new OrderSplit("\\t"), new Fields("order_id", "order_amt", "create_date", "province_id"))
                .shuffle()
                .groupBy(new Fields("create_date", "province_id"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("order_id"), new Count(), new Fields("order_num"));

        topology.newDRPCStream("getOrderNum", drpc).parallelismHint(1)
                .each(new Fields("args"), new Split(" "), new Fields("arg"))
                .each(new Fields("arg"), new SplitBy("\\:"), new Fields("create_date", "province_id"))
                .groupBy(new Fields("create_date", "province_id"))
                .stateQuery(orderState, new Fields("create_date", "province_id"), new MapGet(), new Fields("order_num"))
        ;

        Config conf = new Config();
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("myTopo", conf, topology.build());


        String countDate = DateFmt.getCountDate(null, DateFmt.date_short);


        while (true) {
            System.err.println("销售额：" + drpc.execute("getOrderAmt", countDate + ":1 " + countDate + ":2 " + countDate + ":3 " + countDate + ":4 " + countDate + ":5 " + countDate + ":6 " + countDate + ":7 " + countDate + ":8"));
            System.err.println("订单数：" + drpc.execute("getOrderNum", countDate + ":1 " + countDate + ":2 " + countDate + ":3 " + countDate + ":4 " + countDate + ":5 " + countDate + ":6 " + countDate + ":7 " + countDate + ":8"));
            Utils.sleep(5000);
        }

    }


}
