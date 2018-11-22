package com.moyiguket.flink.issue.common;

import com.alibaba.fastjson.JSON;
import java.io.IOException;
import java.util.List;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Created by sunda on 2018/11/21.
 */
public class MockDataListSchema implements DeserializationSchema<List<MockData>>,
    SerializationSchema<List<MockData>> {


  @Override
  public List<MockData> deserialize(byte[] bytes) throws IOException {
    String json = new String(bytes);
    List<MockData> mockData = JSON.parseArray(json, MockData.class);
    return mockData;
  }

  @Override
  public boolean isEndOfStream(List<MockData> mockData) {
    return false;
  }

  @Override
  public byte[] serialize(List<MockData> mockData) {
    String jsonString = JSON.toJSONString(mockData);
    return jsonString.getBytes();
  }

  @Override
  public TypeInformation<List<MockData>> getProducedType() {
    TypeInformation<List<MockData>> typeInformation =
        TypeInformation.of(new TypeHint<List<MockData>>() {});
    return typeInformation;
  }
}
