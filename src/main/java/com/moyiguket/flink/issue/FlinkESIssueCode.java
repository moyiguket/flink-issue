package com.moyiguket.flink.issue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

/**
 * Created by sunda on 2018/10/24.
 */
public class FlinkESIssueCode {

  public static void dataToES() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.enableCheckpointing(100); //
    DataStreamSource<String> input = env.fromCollection(Collections.singleton("test-test"));
    List<HttpHost> httpHosts = new ArrayList<>();
    httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
    ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
        httpHosts,
        new ElasticsearchSinkFunction<String>()  {
          public IndexRequest createIndexRequest(String element) {
            Map<String, String> json = new HashMap<>();
            json.put("data", element);
            return Requests.indexRequest()
                .index("twitter")
                .type("_doc")
                .id(String.valueOf(123))
                .source(json);
          }
          @Override
          public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
            indexer.add(createIndexRequest(element));
          }
        }
    );
    esSinkBuilder.setBulkFlushMaxActions(1);
    input.addSink(esSinkBuilder.build());
    env.execute("test es string insert");
  }
}
