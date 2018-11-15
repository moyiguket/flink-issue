package com.moyiguket.flink.issue.timewindow;

import java.lang.invoke.MethodHandles;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sunda on 2018/11/15.
 */
public class FlinkEventTimeCountFunction extends ProcessWindowFunction<MockData, Tuple2<String,Long>,Tuple, TimeWindow> {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  @Override
  public void process(Tuple tuple, Context context, Iterable<MockData> iterable,
      Collector<Tuple2<String, Long>> collector) throws Exception {
    logger.info(String.valueOf(tuple));
  }
}
