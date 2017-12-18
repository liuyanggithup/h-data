/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.productor;

import java.util.Properties;
import java.util.Random;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.kafka.common.utils.Utils;
import tools.DateFmt;

public class Producer extends Thread {
	private final kafka.javaapi.producer.Producer<Integer, String> producer;
	private final String topic;
	private final Properties props = new Properties();

	public Producer(String topic) {
		props.put("serializer.class", "kafka.serializer.StringEncoder");// 字符串消息
		props.put("metadata.broker.list",KafkaProperties.broker_list);
		producer = new kafka.javaapi.producer.Producer<Integer, String>(
				new ProducerConfig(props));
		this.topic = topic;
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
			producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
			Utils.sleep(100) ;
//			if (i==500) {
//				break;
//			}
		}

	}

	public static void main(String[] args) {
		Producer producerThread = new Producer(KafkaProperties.Order_topic);
		producerThread.start();
	}
}
