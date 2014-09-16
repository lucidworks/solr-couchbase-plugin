package org.apache.solr.couchbase;

import java.util.Map;

public class Bucket {
  
  String name;
  String splitpath;
  /** A field mapping for documents from Couchbase. Reversed mapping: target -> source*/
  Map<String,String> fieldmapping;
  
  public Bucket(String name, String splitpath, Map<String,String> fieldmapping) {
    this.name = name;
    this.splitpath = splitpath;
    this.fieldmapping = fieldmapping;
  }

  public String getName() {
    return name;
  }

  public String getSplitpath() {
    return splitpath;
  }

  public Map<String, String> getFieldmapping() {
    return fieldmapping;
  }
}
