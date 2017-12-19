package kafka.consumers;

import kafka.productor.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OrderConsumer extends Thread {
    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private Queue<String> queue = new ConcurrentLinkedQueue<String>();

    public OrderConsumer(String topic) {
        consumer = createConsumer();
        this.topic = topic;
    }

    private static KafkaConsumer createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.25.102:9092");
        props.put("group.id", KafkaProperties.groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);

    }

    public static void main(String[] args) {
        OrderConsumer consumerThread = new OrderConsumer(KafkaProperties.Order_topic);
        consumerThread.start();
    }

    public Queue<String> getQueue() {
        return queue;
    }

    public void run() {
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {

                queue.add(record.value());
                System.out.println("消费数据：" + record.value());

            }
        }
    }
}

