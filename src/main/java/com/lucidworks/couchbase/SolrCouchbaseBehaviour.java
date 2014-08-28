package com.lucidworks.couchbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.couchbase.capi.CouchbaseBehavior;

public class SolrCouchbaseBehaviour implements CouchbaseBehavior{
  
  String host = "127.0.0.1";
  int port = 9876;
  String bucketName = "solr-couchbase-plugin";

  public List<String> getPools() {
    List<String> result = new ArrayList<String>();
    result.add("default");
    return result;
  }

  public String getPoolUUID(String pool) {
      return "00000000000000000000000000000000";
  }
  
  public Map<String, Object> getPoolDetails(String pool) {
      Map<String, Object> bucket = new HashMap<String, Object>();
      bucket.put("uri", "/pools/" + pool + "/buckets?uuid=" + getPoolUUID(pool));
  
      Map<String, Object> responseMap = new HashMap<String, Object>();
      responseMap.put("buckets", bucket);
      
      List<Object> nodes = getNodesServingPool(pool);
      responseMap.put("nodes", nodes);
  
      return responseMap;
  }
  
  public List<String> getBucketsInPool(String pool) {
      List<String> bucketNameList = new ArrayList<String>();
      bucketNameList.add("default");
  
      return bucketNameList;
  }
  
  public String getBucketUUID(String pool, String bucket) {
      if("default".equals(bucket)) {
          return "00000000000000000000000000000000";
      }
      return null;
  }
  
  public List<Object> getNodesServingPool(String pool) {
      List<Object> nodes = null;
      if("default".equals(pool)) {
          nodes = new ArrayList<Object>();
  
          Map<String, Object> nodePorts = new HashMap<String, Object>();
//          nodePorts.put("direct", 8091);
          nodePorts.put("direct", port);
  
          Map<String, Object> node = new HashMap<String, Object>();
//          node.put(bucketName,
          node.put("couchApiBase",
                  String.format("http://%s:%s/", host, port));
//          node.put("hostname", 8091);
          node.put("hostname", host + ":" + port);
          node.put("ports", nodePorts);
  
          nodes.add(node);
  
//          Map<String, Object> nodePorts2 = new HashMap<String, Object>();
//          nodePorts2.put("direct", 8091);
//  
//          Map<String, Object> node2 = new HashMap<String, Object>();
//          node2.put("couchApiBase",
//                  String.format("http://%s/%s", "127.0.0.2", "default"));
//          node2.put("hostname", 8091);
//          node2.put("ports", nodePorts2);
//  
//          nodes.add(node2);
      }
  
      return nodes;
  }
  
  @Override
  public Map<String, Object> getStats() {
      return new HashMap<String, Object>();
  }
  
}
