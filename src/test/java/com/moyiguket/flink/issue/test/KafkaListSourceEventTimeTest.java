package com.moyiguket.flink.issue.test;

import com.alibaba.fastjson.JSON;
import com.moyiguket.flink.issue.common.MockData;
import com.moyiguket.flink.issue.common.MockDataListSchema;
import com.moyiguket.flink.issue.source.KafkaSource;
import com.moyiguket.flink.issue.timewindow.FlinkEventTimeCountFunction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.junit.Test;

/**
 * Created by sunda on 2018/11/21.
 */
public class KafkaListSourceEventTimeTest {

  private static final String broker_url = "localhost:9092";
  private static final String topic = "mock_list";

  @Test
  public void sinkData() throws Exception {
    FlinkKafkaProducer011<List<MockData>> producer011 =
        new FlinkKafkaProducer011<>(broker_url, topic, new MockDataListSchema());

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(100); //
    AtomicInteger maxNum = new AtomicInteger(Integer.MAX_VALUE);
    AtomicInteger genIdx = new AtomicInteger();
    env.addSource(new SourceFunction<List<MockData>>() {
      @Override
      public void run(SourceContext<List<MockData>> ctx) throws Exception {
        while (genIdx.get() < maxNum.get()) {
          List<MockData> mockDataList = new ArrayList<>();
          for (int i = 0; i < 3; i++) {
            MockData mockData = new MockData();
            mockData.setAge(ThreadLocalRandom.current().nextInt(1, 99));
            mockData.setCountry("country" + ThreadLocalRandom.current().nextInt(3, 443));
            mockData.setId(genIdx.get());
            mockDataList.add(mockData);
            mockData.setTimestamp(System.currentTimeMillis()-300);
          }
          ctx.collectWithTimestamp(mockDataList, System.currentTimeMillis());
          genIdx.incrementAndGet();
          TimeUnit.SECONDS.sleep(5);
        }
      }

      @Override
      public void cancel() {
        genIdx.set(Integer.MAX_VALUE);
      }
    }).addSink(producer011);

    env.execute("sink data");

  }


  @Test
  public void testKafkaListEventTime() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(100); //

    DataStreamSource<List<MockData>> listDataStreamSource = KafkaSource.genStream(env);
    SingleOutputStreamOperator<MockData> convertToPojo = listDataStreamSource
        .process(new ProcessFunction<List<MockData>, MockData>() {
          @Override
          public void processElement(List<MockData> value, Context ctx, Collector<MockData> out)
              throws Exception {
            value.forEach(mockData -> out.collect(mockData));
          }
        });
    convertToPojo.assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor<MockData>(Time.seconds(5)) {
          @Override
          public long extractTimestamp(MockData element) {
            return element.getTimestamp();
          }
        });
    SingleOutputStreamOperator<Tuple2<String, Long>> countStream = convertToPojo
        .keyBy("country").window(
            SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(10)))
        .process(
            new FlinkEventTimeCountFunction()).name("count elements");

    countStream.addSink(new SinkFunction<Tuple2<String, Long>>() {
      @Override
      public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
        System.out.println(value);
      }
    });



    env.execute("test event time");
  }
}
