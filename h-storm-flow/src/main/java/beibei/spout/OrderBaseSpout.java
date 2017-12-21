package beibei.spout;

import kafka.consumers.OrderConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 基础的订单数据Spout
 */
public class OrderBaseSpout implements IRichSpout {

    //topic
    String topic = null;
    Integer TaskId = null;
    SpoutOutputCollector collector = null;
    Queue<String> queue = new ConcurrentLinkedQueue<String>();

    //spout的构造器，传入topic
    public OrderBaseSpout(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        //获取topology任务ID
        TaskId = context.getThisTaskId();
        //创建消费者消费数据
        OrderConsumer consumer = new OrderConsumer(topic);
        consumer.start();
        //为队列赋值
        queue = consumer.getQueue();
    }

    @Override
    public void nextTuple() {

        /**
         * 如果队列中有数据
         */
        if (queue.size() > 0) {
            //从队列中取出数据
            String str = queue.poll();
            //进行数据过滤
            System.out.println("TaskId:" + TaskId + ";  str=" + str);
            //将数据交由Bolt处理
            collector.emit(new Values(str));
        }

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("order"));
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }


    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
