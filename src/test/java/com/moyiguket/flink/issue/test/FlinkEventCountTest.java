package com.moyiguket.flink.issue.test;

import com.moyiguket.flink.issue.timewindow.FlinkEventTimeCountFunction;
import com.moyiguket.flink.issue.timewindow.MockData;
import com.moyiguket.flink.issue.timewindow.DataMockSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
    env.enableCheckpointing(100); //

    DataStreamSource<MockData> mockDataDataStreamSource = env.addSource(new DataMockSource());

    mockDataDataStreamSource.keyBy("country").window(
        SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(20), Time.hours(-8)))
        .allowedLateness(Time.seconds(5))
        .process(
            new FlinkEventTimeCountFunction()).name("count elements");

    env.execute("count test ");
  }
}
