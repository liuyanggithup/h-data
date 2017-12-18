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
import org.apache.storm.utils.Utils;
import tools.DateFmt;

public class LogProducer extends Thread {
	private final kafka.javaapi.producer.Producer<Integer, String> producer;
	private final String topic;
	private final Properties props = new Properties();

	public LogProducer(String topic) {
		props.put("serializer.class", "kafka.serializer.StringEncoder");// 字符串消息
		props.put("metadata.broker.list",KafkaProperties.broker_list);
		producer = new kafka.javaapi.producer.Producer<Integer, String>(
				new ProducerConfig(props));
		this.topic = topic;
	}

	public void run() {
		// url, session_id, time  -- provinceId, track_u ...
		Random random = new Random();
		String[] hosts = { "www.taobao.com" };
		String[] session_id = { "ABYH6Y4V4SCVXTG6DPB4VH9U123", "XXYH6YCGFJYERTT834R52FDXV9U34", "BBYH61456FGHHJ7JL89RG5VV9UYU7",
				"CYYH6Y2345GHI899OFG4V9U567", "VVVYH6Y4V4SFXZ56JI111PDPB4V678"};
		
		int i =0 ;
		while(true) {
			i ++ ;
			String sidString = session_id[random.nextInt(5)]+i ;
			String messageStr1 = hosts[0]+"\t"+sidString+"\t"+ DateFmt.getCountDate(null, DateFmt.date_long);
			String messageStr2 = hosts[0]+"\t"+sidString+"\t"+DateFmt.getCountDate(null, DateFmt.date_long);
			producer.send(new KeyedMessage<Integer, String>(topic, messageStr1));
			producer.send(new KeyedMessage<Integer, String>(topic, messageStr2));
			System.out.println(messageStr1);
			Utils.sleep(100) ;
		}
	}

	public static void main(String[] args) {
		LogProducer producerThread = new LogProducer(KafkaProperties.Log_topic);
		producerThread.start();
	}
}
