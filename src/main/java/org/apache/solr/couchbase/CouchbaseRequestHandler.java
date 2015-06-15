package org.apache.solr.couchbase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.couchbase.common.CommonConstants;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CouchbaseRequestHandler extends RequestHandlerBase implements SolrCoreAware {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseRequestHandler.class);

  public static final String CAPISERVER_PATH = "/capi_servers";
  
  private CouchbaseReplica couchbase;
  
  private SolrCore core;
  private UpdateRequestProcessorChain processorChain;
  private SolrZkClient zkClient;
  private ZkStateReader zkStateReader;
  private ElectionWatcher electionWatcher;
  private String collection;
  
  private NamedList couchbaseServer = null;
  
  @Override
  public String getSource() { return null; }

  @Override
  public void inform(SolrCore core) {
    this.core = core;
    SolrQueryRequest req = new SolrQueryRequestBase(getCore(), new SolrParams() {
      
      @Override
      public String[] getParams(String param) {
        // TODO Auto-generated method stub
        return null;
      }
      
      @Override
      public Iterator<String> getParameterNamesIterator() {
        // TODO Auto-generated method stub
        return null;
      }
      
      @Override
      public String get(String param) {
        // TODO Auto-generated method stub
        return null;
      }
    }) {};
    SolrQueryResponse rsp = new SolrQueryResponse();
    processorChain =
        getCore().getUpdateProcessingChain("");
    zkClient = getZkClient();
    collection = getCore().getName();
  }
  
  @Override
  public void init(NamedList args) {
    super.init(args);
    Map<String,Object> params = toMap((NamedList<String>)args.get(CommonConstants.HANDLER_PARAMS));
    List<NamedList<Object>> bucketsList = args.getAll(CommonConstants.BUCKET_MARK);
    couchbaseServer = (NamedList)params.get(CommonConstants.COUCHBASE_SERVER_FIELD);
    if(couchbaseServer == null) {
      LOG.info("No Couchbase server configured!");
    }
    else if(couchbaseServer != null && couchbaseServer.size() <= 0) {
      LOG.error("Missing content for Couchbase server!");
    }
    
    couchbase = new CouchbaseReplica(this, params, bucketsList);
  }
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
      throws Exception {
    SolrParams params = req.getParams();
    String action = params.get(CommonParams.ACTION);
    
    action = action.toLowerCase();
    action = action.trim();
    
    switch(action) {
    case "start"  :
    	handleStart();
      break;
    case "stop" :
      handleStop();
      break;
    }
  }

  @Override
  public String getDescription() {
    return "Couchbase plugin";
  }
  
  public SolrZkClient getZkClient() {
    SolrZkClient client = null;
    if(getCore() != null) {
      CoreContainer container = getCore().getCoreDescriptor().getCoreContainer();
      if(container.isZooKeeperAware()) {
        client = container.getZkController().getZkClient();
      }
    }
    return client;
  }
  
  public boolean isSolrCloud() {
    return getCore().getCoreDescriptor().getCoreContainer().isZooKeeperAware();
  }
  
  public void handleStart() {
    if(!couchbase.isRunning()) {
      //Check if in SolrCloud mode
      if(isSolrCloud()) {
        checkIfIamLeader();
      } else {
        couchbase.startCouchbaseReplica();
        
        //setup XDCR
        String clientUuid = couchbase.getRemoteClusterUuid();
        boolean exists = couchbase.checkRemoteClusterExists(clientUuid);
        if(!exists) {
          exists = couchbase.createRemoteCluster(clientUuid);
        }
        if(exists) {
          try {
            couchbase.createReplication(clientUuid);
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
          }
        }
        while(couchbase.isRunning()) {
          try {
            Thread.sleep(10000);
          } catch (InterruptedException e) {
          }
        }
        try {
          couchbase.close();
          if(!couchbase.isRunning()) {
            LOG.info("Couchbase Replica STOPPED successfully.");
          }
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    } else {
      LOG.info("Couchbase Replica is already RUNNING.");
    }
  }
  
  public void handleStop() {
    if(couchbase.isRunning()) {
      couchbase.stopCouchbaseReplica();
    } else {
      LOG.info("CAPIServer is not running.");
    }
  }
  
  public void checkIfIamLeader() {
    //at first, check if I am the first shard leader, which shoudld start Couchbase replica
    zkStateReader = new ZkStateReader(getZkClient());
    try {
      zkStateReader.updateClusterState(true);
    } catch (KeeperException | InterruptedException e1) {
      LOG.error("Error while updating Cluster State!", e1);
    }
    String collection = getCore().getCoreDescriptor().getCloudDescriptor().getCollectionName();
    Map<String,Slice> slices = zkStateReader.getClusterState().getActiveSlicesMap(collection);
    List<String> sliceNames = new ArrayList<String>(slices.keySet());
    Collections.sort(sliceNames);
    String shard = sliceNames.get(0);
    Replica replica = null;
    try {
      replica = zkStateReader.getLeaderRetry(collection, shard);
    } catch (InterruptedException e) {
      LOG.error("Could not get leader!", e);
    }
    final String coreNodeName = getCore().getCoreDescriptor().getCloudDescriptor().getCoreNodeName();
    if(replica != null && coreNodeName != null && replica.getName().equals(coreNodeName)) { // I am the leader
      couchbase.startCouchbaseReplica();
    } else { // I'm not the leader, watch leader.
      String watchedNode = "/collections/" + collection + "/leaders/shard1";
      electionWatcher = new ElectionWatcher(coreNodeName, watchedNode);
      try {
        zkClient.getData(watchedNode, electionWatcher, null, true);
      } catch (KeeperException e) {
        LOG.warn("Failed setting watch", e);
        // we couldn't set our watch - the node before us may already be down?
        // we need to check if we are the leader again
        checkIfIamLeader();
      } catch (InterruptedException e) {
        LOG.warn("Failed setting watch", e);
      }
    }
  }
  
  public UpdateRequestProcessorChain getProcessorChain() {
    return this.processorChain;
  }
  
  public SolrCore getCore() {
    return this.core;
  }
  
  public CouchbaseReplica getCouchbaseReplica() {
    return couchbase;
  }
  
  /** Create a Map&lt;String,Object&gt; from a NamedList given no keys are repeated */
  public static Map<String,Object> toMap(NamedList params) {
    HashMap<String,Object> map = new HashMap<>();
    for (int i=0; i<params.size(); i++) {
      map.put(params.getName(i), params.getVal(i));
    }
    return map;
  }
  
  public Map <String,ZkNodeProps> getCollectionsLeaders() {
    List<String> capiservers = new ArrayList<String>();
    Map<String,ZkNodeProps> capiserversProps = new HashMap<String, ZkNodeProps>();
    try {
      zkStateReader.updateClusterState(true);
      capiservers = zkClient.getChildren(CAPISERVER_PATH, null, true);
      for(String serverName : capiservers) {
        ZkNodeProps props = ZkNodeProps.load(zkClient.getData(
            CAPISERVER_PATH + "/" + serverName, null, null, true));
        capiserversProps.put(serverName, props);
      }
    } catch (KeeperException | InterruptedException e1) {
      LOG.error("Error while updating Cluster State!", e1);
    }
//    ClusterState state = zkStateReader.getClusterState();
//    Set<String> collections = state.getCollections();
//    for(String collection : collections) {
//      List<Slice> activeSlices = new ArrayList<Slice>(state.getActiveSlices(collection));
//      for(Slice slice : activeSlices) {
//        Replica replica = slice.getLeader();
//        Map<String,Object> properties = replica.getProperties();
//        String baseUrl = (String) properties.get("base_url");
//        String[] splits = baseUrl.split(":");
//        String host = splits[1].substring(splits[1].lastIndexOf("/")+1, splits[1].length());
//        String port = splits[2].substring(0, splits[2].indexOf("/"));
//        leaders.put(host, port);
//      }
//    }
    return capiserversProps;
  }
  
  private class ElectionWatcher implements Watcher {
    
    final String myNode,watchedNode;
    private boolean canceled = false;
    
    private ElectionWatcher(String myNode, String watchedNode) {
      this.myNode = myNode;
      this.watchedNode = watchedNode;
    }
    
    void cancel(String leaderSeqPath){
      canceled = true;
    }
    
    @Override
    public void process(WatchedEvent event) {
      if (EventType.None.equals(event.getType())) {
        return;
      }
      if (canceled) {
        LOG.info("This watcher is not active anymore {}", myNode);
        try {
          zkClient.delete(myNode, -1, true);
        } catch (KeeperException.NoNodeException nne) {
          // expected . don't do anything
        } catch (Exception e) {
          LOG.warn("My watched node still exists and can't remove " + myNode, e);
        }
        return;
      }
      try {
        // am I the next leader?
        checkIfIamLeader();
      } catch (Exception e) {
        LOG.warn("", e);
      }
    }
    
  }
}
