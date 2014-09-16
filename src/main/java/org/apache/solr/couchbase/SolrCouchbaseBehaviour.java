package org.apache.solr.couchbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.couchbase.capi.CouchbaseBehavior;

public class SolrCouchbaseBehaviour implements CouchbaseBehavior{

  CouchbaseRequestHandler handler;
  
  public SolrCouchbaseBehaviour(CouchbaseRequestHandler handler) {
    this.handler = handler;
  }
  
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
      return new ArrayList<String>(handler.getBuckets().keySet());
  }
  
  public String getBucketUUID(String pool, String bucket) {
      if(handler.getBucket(bucket) != null) {
          return "00000000000000000000000000000000";
      }
      return null;
  }
  
  public List<Object> getNodesServingPool(String pool) {
      List<Object> nodes = null;
      if("default".equals(pool)) {
          nodes = new ArrayList<Object>();
  
          Map<String, Object> nodePorts = new HashMap<String, Object>();
          nodePorts.put("direct", handler.getPort());
  
          Map<String, Object> node = new HashMap<String, Object>();
          node.put("couchApiBase",
                  String.format("http://%s:%s/", handler.getHost(), handler.getPort()));
          node.put("hostname", handler.getHost() + ":" + handler.getPort());
          node.put("ports", nodePorts);
  
          nodes.add(node);
      }
  
      return nodes;
  }
  
  @Override
  public Map<String, Object> getStats() {
      return new HashMap<String, Object>();
  }
  
}
