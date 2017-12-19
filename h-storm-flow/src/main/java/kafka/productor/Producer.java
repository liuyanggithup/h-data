/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.productor;

import kafka.producer.KeyedMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import tools.DateFmt;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;


public class Producer extends Thread {
    private final org.apache.kafka.clients.producer.Producer<Integer, String> producer;
    private final String topic;
    private final Properties props = new Properties();

    public Producer(String topic) {
        props.put("bootstrap.servers", "192.168.25.102:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer(props, new StringSerializer(), new StringSerializer());
        this.topic = topic;
    }

    public static void main(String[] args) {

        Producer producerThread = new Producer(KafkaProperties.topic);
        producerThread.start();
    }

    public void run() {


        // order_id,order_amt,create_time,province_id
        Random random = new Random();
        String[] order_amt = { "10.10", "20.10", "50.2","60.0", "80.1" };
        String[] province_id = { "1","2","3","4","5","6","7","8" };
        int i =0 ;
        while(true) {
            i ++ ;
            String messageStr = i+"\t"+order_amt[random.nextInt(5)]+"\t"+ DateFmt.getCountDate(null, DateFmt.date_long)+"\t"+province_id[random.nextInt(8)] ;
            System.out.println("product:"+messageStr);
            producer.send(new ProducerRecord(topic,messageStr));
            Utils.sleep(100) ;
			if (i==500) {
				break;
			}
        }



    }
}
