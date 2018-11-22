package com.moyiguket.flink.issue.source;

import com.moyiguket.flink.issue.common.MockData;
import com.moyiguket.flink.issue.common.MockDataListSchema;
import java.util.List;
import java.util.Properties;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Created by sunda on 2018/11/21.
 */
public class KafkaSource {

  private static final String broker_url="localhost:9092";
  private static final String topic="mock_list";
  public static DataStreamSource<List<MockData>> genStream(StreamExecutionEnvironment env){
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,broker_url);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
    FlinkKafkaConsumer011<List<MockData>> kafkaConsumer011 =
        new FlinkKafkaConsumer011<>(topic, new MockDataListSchema(), properties);

    DataStreamSource<List<MockData>> listDataStreamSource = env.addSource(kafkaConsumer011);
    return listDataStreamSource;
  }



}
