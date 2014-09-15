package org.apache.solr.cloud;

import java.io.IOException;
import org.apache.solr.cloud.ElectionContext;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.KeeperException;

import com.lucidworks.couchbase.CouchbaseRequestHandler;

public class CouchbaseElectionContext extends ElectionContext{
  
  private CouchbaseRequestHandler handler;
  public static final String PATH = "/couchbase_elect";
  public static final String LEADER_PATH = "/couchbase_elect/leader";

  public CouchbaseElectionContext(CouchbaseRequestHandler handler, final String coreNodeName,
      final SolrZkClient zkClient) {
    super(coreNodeName, PATH, LEADER_PATH, null, zkClient);
    this.handler = handler;
  }
  
  void runLeaderProcess(boolean weAreReplacement, int pauseBeforeStartMs)
      throws KeeperException, InterruptedException, IOException {
    handler.startCouchbaseReplica();
  }


}
