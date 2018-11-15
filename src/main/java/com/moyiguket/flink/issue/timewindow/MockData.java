package com.moyiguket.flink.issue.timewindow;

import java.io.Serializable;

/**
 * Created by sunda on 2018/11/15.
 */
public class MockData implements Serializable {
  private long id;

  private long timestamp;

  private String name;

  private int age;

  private String country;


  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public String getCountry() {
    return country;
  }

  public void setCountry(String country) {
    this.country = country;
  }
}
