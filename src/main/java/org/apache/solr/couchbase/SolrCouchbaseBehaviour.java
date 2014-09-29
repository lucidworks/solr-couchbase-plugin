package org.apache.solr.couchbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.cloud.ZkNodeProps;

import com.couchbase.capi.CouchbaseBehavior;

public class SolrCouchbaseBehaviour implements CouchbaseBehavior{

  CouchbaseRequestHandler handler;
  String poolUUID;
  
  public SolrCouchbaseBehaviour(CouchbaseRequestHandler handler) {
    this.handler = handler;
    poolUUID = Utils.randomID();
  }
  
  public List<String> getPools() {
    List<String> result = new ArrayList<String>();
    result.add("default");
    return result;
  }

  public String getPoolUUID(String pool) {
      return poolUUID;
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
      Map<String,ZkNodeProps> capiserversProps = handler.getCollectionsLeaders();
      if("default".equals(pool)) {
        nodes = new ArrayList<Object>();
        for(Map.Entry<String, ZkNodeProps> entry : capiserversProps.entrySet()) {
          String host = entry.getValue().getStr("host");
          int port = entry.getValue().getInt("port", 9999);
  
          Map<String, Object> nodePorts = new HashMap<String, Object>();
          nodePorts.put("direct", port);
  
          Map<String, Object> node = new HashMap<String, Object>();
          node.put("couchApiBase",
                  String.format("http://%s:%s/", host, port));
          node.put("hostname", host + ":" + port);
          node.put("ports", nodePorts);
  
          nodes.add(node);
        }
      }
  
      return nodes;
  }
  
  @Override
  public Map<String, Object> getStats() {
      return new HashMap<String, Object>();
  }
  
}
