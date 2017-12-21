package beibei.trident;


import beibei.trident.function.Print;
import kafka.api.OffsetRequest;
import kafka.productor.KafkaProperties;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.tuple.Fields;

public class TridentTopoExample {


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
        topology.newStream("spout", spout).parallelismHint(1).each(new Fields(StringScheme.STRING_SCHEME_KEY), new Print(), new Fields("msg"));
        Config conf = new Config();
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("myTopo", conf, topology.build());


    }


}
