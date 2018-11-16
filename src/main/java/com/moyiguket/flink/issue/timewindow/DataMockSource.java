package com.moyiguket.flink.issue.timewindow;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Created by sunda on 2018/11/15.
 */
public class DataMockSource implements SourceFunction<MockData> {
  private volatile boolean running = true;
  @Override
  public void run(SourceContext sourceContext) throws Exception {
    while (running){
      MockData mockData = new MockData();
      mockData.setAge(ThreadLocalRandom.current().nextInt(1,99));
      mockData.setCountry("country "+ThreadLocalRandom.current().nextInt(2,5));
      mockData.setId(ThreadLocalRandom.current().nextLong());
      mockData.setTimestamp(Instant.now().toEpochMilli());
      sourceContext.collectWithTimestamp(mockData,Instant.now().toEpochMilli());
//      sourceContext.collect(mockData);

      TimeUnit.SECONDS.sleep(10);
    }
  }

  @Override
  public void cancel() {
     running = false;
  }
}
