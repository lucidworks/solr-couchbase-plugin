package com.lucidworks.couchbase;

public class Counter {

  private long value;
  private int count;
  
  public Counter() {
    this.value = 0;
    this.count = 0;
  }
  
  public void inc() {
    value += 1;
    count += 1;
  }
  
  public void dec() {
    value -= 1;
    count -= 1;
  }
  
  public void inc(long num) {
    value += num;
    count += 1;
  }
  
  public long sum() {
    return value;
  }
  
  public float mean() {
    return value/count;
  }
  
  public int count() {
    return count;
  }
}
