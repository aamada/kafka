/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples.me;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.PrintUitls;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.PrintUitls.printToConsole;

/**
 * ;broker, producer, consumer
 * topic
 * partition
 * replicator
 * ar:
 * ir
 * or
 * pull
 * push
 */
public class KafkaProducerDemo1 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", Constant.BROKER_LIST);


        KafkaProducer<String, String> producer =
            new KafkaProducer<>(properties);
        ProducerRecord<String, String> record =
            new ProducerRecord<>(Constant.TOPIC, "hello, Kafka!");
        try {
            printToConsole("用户了一条消息去了");
            Future<RecordMetadata> send = producer.send(record);
            printToConsole("等待第一次发送的结果");
            send.get();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            printToConsole("用户了再发了一条消息去了");
            Future<RecordMetadata> ose = producer.send(record);
            printToConsole("等待第2次发送的结果");
            ose.get();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        producer.close();
    }
}
