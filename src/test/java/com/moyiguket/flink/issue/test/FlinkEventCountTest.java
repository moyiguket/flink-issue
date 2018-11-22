package com.moyiguket.flink.issue.test;

import com.moyiguket.flink.issue.timewindow.FlinkEventTimeCountFunction;
import com.moyiguket.flink.issue.common.MockData;
import com.moyiguket.flink.issue.timewindow.DataMockSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;

/**
 * Created by sunda on 2018/11/15.
 */
public class FlinkEventCountTest {

  @Test
  public void testProcessWindow() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(100); //

    DataStreamSource<MockData> mockDataDataStreamSource = env.addSource(new DataMockSource());
    mockDataDataStreamSource.assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor<MockData>(Time.seconds(10)) {
          @Override
          public long extractTimestamp(MockData mockData) {
            return mockData.getTimestamp();
          }
        });

    mockDataDataStreamSource.keyBy("country").window(
        SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(20), Time.hours(-8)))
        .allowedLateness(Time.seconds(5))
        .process(
            new FlinkEventTimeCountFunction()).name("count elements");

    env.execute("count test ");
  }


  @Test
  public void testEventTime() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(10000); //

    DataStreamSource<MockData> mockDataDataStreamSource = env.addSource(new DataMockSource());
    mockDataDataStreamSource.assignTimestampsAndWatermarks(
        new AscendingTimestampExtractor<MockData>() {
          @Override
          public long extractAscendingTimestamp(MockData element) {
            return element.getTimestamp();
          }
        });

    SingleOutputStreamOperator<Tuple2<String, Long>> countStream = mockDataDataStreamSource
        .keyBy("country").window(
            SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(10)))
//        .allowedLateness(Time.seconds(5))
        .process(
            new FlinkEventTimeCountFunction()).name("count elements");

    countStream.addSink(new SinkFunction<Tuple2<String, Long>>() {
      @Override
      public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
        System.out.println(value);
      }
    });

    env.execute("count test ");
  }
}
