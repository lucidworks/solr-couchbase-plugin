package org.apache.solr.couchbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import junit.framework.TestCase;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.couchbase.common.CommonConstants;
import org.junit.Test;

public class CouchbaseReplicaTest extends TestCase{
  
  CouchbaseReplica replica;

  @Test
  public void testXDCR() throws Exception {
    replica = createCouchbaseReplica();
    replica.startCouchbaseReplica();
    assertTrue(replica.isRunning());
    //setup XDCR
    String clientUuid = replica.getRemoteClusterUuid();
    boolean exists = replica.checkRemoteClusterExists(clientUuid);
    if(!exists) {
      exists = replica.createRemoteCluster(clientUuid);
    }
    if(exists) {
      List<String> replications = replica.createReplication(clientUuid);
      assertTrue(replications.size() > 0);
    }
    replica.close();
  }
  
  public CouchbaseReplica createCouchbaseReplica() {
    MockRequestHandler handler = new MockRequestHandler();
    
    Map<String,Object> params = new HashMap<String, Object>();
    params.put(CommonConstants.USERNAME_FIELD, "Administrator");
    params.put(CommonConstants.PASSWORD_FIELD, "password");
    params.put(CommonConstants.SERVER_HOST, "127.0.0.1");
    params.put(CommonConstants.SERVER_PORT, 8091);
    params.put(CommonConstants.CLIENT_HOST, "127.0.0.1");
    params.put(CommonConstants.CLIENT_PORT, 9876);
    params.put(CommonConstants.NUM_VBUCKETS_FIELD, 64);
    params.put(CommonConstants.COMMIT_AFTER_BATCH_FIELD, true);
    params.put(CommonConstants.COUCHBASE_CLUSTER_NAME_FIELD, getName() + "-" + UUID.randomUUID().toString().replaceAll("-", ""));
    params.put(CommonConstants.USERNAME_FIELD, "Administrator");
    
    List<NamedList<Object>> bucketsList = new ArrayList<NamedList<Object>>();
    NamedList<Object> bucket1 = new NamedList<Object>();
    bucket1.add(CommonConstants.NAME_FIELD, "beer-sample");
    bucket1.add(CommonConstants.SPLITPATH_FIELD, "/");
    NamedList<Object> fieldmappings1 = new NamedList<Object>();
    fieldmappings1.add("address", "address_ss:/address");
    fieldmappings1.add("all", "/*");
    bucket1.add(CommonConstants.FIELD_MAPPING_FIELD, fieldmappings1);
    bucketsList.add(bucket1);
    
    CouchbaseReplica replica = new CouchbaseReplica(handler, params, bucketsList);
    return replica;
  }
}
