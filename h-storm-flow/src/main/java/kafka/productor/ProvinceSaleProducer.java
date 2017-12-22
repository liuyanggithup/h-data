package kafka.productor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.utils.Utils;
import tools.DateFmt;

import java.util.Properties;
import java.util.Random;

public class ProvinceSaleProducer extends Thread {

    private final org.apache.kafka.clients.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public ProvinceSaleProducer(String topic) {
        props.put("bootstrap.servers", KafkaProperties.broker_list);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer(props);
        this.topic = topic;
    }

    public static void main(String[] args) {

        ProvinceSaleProducer producerThread = new ProvinceSaleProducer(KafkaProperties.Order_topic);
        producerThread.start();
    }

    public void run() {

        // order_id,order_amt,create_time,province_id
        Random random = new Random();
        String[] order_amt = {"10.10", "20.10", "50.2", "60.0", "80.1"};
        String[] province_id = {"1", "2", "3", "4", "5", "6", "7", "8"};
        int i = 0;
        while (true) {
            i++;
            if (i == 10) {
                break;
            }
            String messageStr = i + "\t" + order_amt[random.nextInt(5)] + "\t" + DateFmt.getCountDate(null, DateFmt.date_long) + "\t" + province_id[random.nextInt(8)];
            System.out.println("product:" + messageStr);
            producer.send(new ProducerRecord(topic, messageStr));
            Utils.sleep(1000);
        }

    }


}
