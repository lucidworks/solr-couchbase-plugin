package org.apache.solr.couchbase;

import java.util.Map;

import junit.framework.TestCase;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.couchbase.common.Bucket;
import org.apache.solr.couchbase.common.CommonConstants;
import org.junit.Test;

public class CouchbaseReplicaTest extends TestCase {
  
  @Test
  public void testReplicaConfig() {
    CouchbaseRequestHandler handler = new CouchbaseRequestHandler();
    handler.init(createTestArgs());
    CouchbaseReplica replica = handler.getCouchbaseReplica();
    assertEquals("Administrator", replica.getServerUsername());
    assertEquals("password", replica.getServerPassword());
    assertEquals("Administrator", replica.getClientUsername());
    assertEquals("password", replica.getClientPassword());
    assertEquals("127.0.0.1", replica.getServerHost());
    assertEquals(8091, replica.getServerPort());
    assertEquals("127.0.0.1", replica.getClientHost());
    assertEquals(9876, replica.getClientPort());
    assertEquals(64, replica.getNumVBuckets());
    assertEquals(true, replica.isCommitAfterBatch());
    Map<String,String> servers = replica.getCouchbaseServers();
    for(Map.Entry<String, String> server : servers.entrySet()) {
      assertTrue(server.getKey().equals("server1") || server.getKey().equals("server2"));
      assertTrue(server.getValue().equals("127.0.0.1:8091") || server.getValue().equals("127.0.0.1:9898"));
    }
    assertEquals("solr", replica.getClusterName());
    Bucket bucket = replica.getBucket("beer-sample");
    assertEquals("/", bucket.getSplitpath());
    assertEquals("address_s:/address", bucket.getFieldmapping().get("address"));
    assertEquals("/*", bucket.getFieldmapping().get("all"));
  }
  
  public NamedList createTestArgs() {
    NamedList args = new NamedList<Object>();
    NamedList params = new NamedList<Object>();
    params.add(CommonConstants.USERNAME_FIELD, "Administrator");
    params.add(CommonConstants.PASSWORD_FIELD, "password");
    params.add(CommonConstants.CLIENT_PORT, 9876);
    params.add(CommonConstants.NUM_VBUCKETS_FIELD, 64);
    params.add(CommonConstants.COMMIT_AFTER_BATCH_FIELD, true);
    NamedList servers = new NamedList<Object>();
    servers.add("server1", "127.0.0.1:8091");
    servers.add("server2", "127.0.0.1:9898");
    params.add(CommonConstants.COUCHBASE_SERVERS_FIELD, servers);
    params.add(CommonConstants.COUCHBASE_CLUSTER_NAME_FIELD, "solr");
    args.add("params", params);
    
    NamedList bucket = new NamedList<Object>();
    bucket.add(CommonConstants.NAME_FIELD, "beer-sample");
    bucket.add(CommonConstants.SPLITPATH_FIELD, "/");
    NamedList fieldmappings = new NamedList<Object>();
    fieldmappings.add("address", "address_s:/address");
    fieldmappings.add("all", "/*");
    bucket.add(CommonConstants.FIELD_MAPPING_FIELD, fieldmappings);
    args.add("bucket", bucket);
    
    return args;
  }
}
