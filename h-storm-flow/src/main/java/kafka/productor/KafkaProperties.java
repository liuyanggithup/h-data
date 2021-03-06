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

public interface KafkaProperties {
    final static String zkConnect = "39.106.120.19:2181";
    final static String broker_list = "39.106.120.19:9092";
    final static String hbase_zkList = "39.106.120.19";


    final static String groupId = "group1";
    final static String topic = "topic";
    final static String Order_topic = "Order_topic";
    final static String Log_topic = "Log_topic";

}
