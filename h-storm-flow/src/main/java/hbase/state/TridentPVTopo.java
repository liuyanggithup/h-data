package hbase.state;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Random;


public class TridentPVTopo {

    public static StormTopology buildTopology(LocalDRPC drpc) {
        Random random = new Random();
        String[] hosts = {"www.taobao.com"};
        String[] session_id = {"ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
                "CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JIPDPB4V678"};
        String[] time = {"2014-01-07 08:40:50", "2014-01-07 08:40:51", "2014-01-09 08:40:52", "2014-01-09 08:40:53",
                "2014-01-07 09:40:49", "2014-01-08 10:40:49", "2014-01-08 11:40:49", "2014-01-09 12:40:49"};

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("eachLog"), 3,
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(8)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(8)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(8)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(8)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(8)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(8)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(8)]),
                new Values(hosts[0] + "\t" + session_id[random.nextInt(5)] + "\t" + time[random.nextInt(8)]));

        spout.setCycle(true);

        TridentConfig config = new TridentConfig("state");

        StateFactory state = HBaseAggregateState.transactional(config);

        TridentTopology topology = new TridentTopology();
        topology.newStream("spout1", spout)
                .each(new Fields("eachLog"), new MySplit("\t"), new Fields("date", "cf", "pv_count", "session_id"))
                .project(new Fields("date", "cf", "pv_count"))
                .groupBy(new Fields("date", "cf", "pv_count"))
                .persistentAggregate(state, new Count(), new Fields("PV"));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(null));
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
        }
    }
}
