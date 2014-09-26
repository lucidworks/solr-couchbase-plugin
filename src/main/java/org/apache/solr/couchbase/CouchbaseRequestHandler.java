package org.apache.solr.couchbase;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.codehaus.jackson.map.ObjectMapper;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.capi.CAPIBehavior;
import com.couchbase.capi.CAPIServer;
import com.couchbase.capi.CouchbaseBehavior;


public class CouchbaseRequestHandler extends RequestHandlerBase implements SolrCoreAware {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseRequestHandler.class);
  private static final String CLUSTER_NAME = "solr-";
  private static final String CLUSTERS_URI = "/pools/default/remoteClusters";
  
  private CouchbaseBehavior couchbaseBehaviour;
  private CAPIBehavior capiBehaviour;
  private CAPIServer server;
  private String host = "127.0.0.1";
  private int port = -1;
  private String username;
  private String password;
  private TypeSelector typeSelector;
  private Settings settings;
  private SolrCore core;
  private UpdateRequestProcessor processor;
  private static SolrZkClient zkClient;
  private ZkStateReader zkStateReader;
  private boolean commitAfterBatch;
  private ElectionWatcher electionWatcher;
  private List<String> couchbaseServersList;
  private String clusterName;
  private CloseableHttpClient httpClient;
  private HttpClientContext httpContext;
  private ObjectMapper mapper = new ObjectMapper();
  private ArrayList<String> clusterNames;

  private Map<String, String> documentTypeParentFields;
  private Map<String, String> documentTypeRoutingFields;
  private Map<String,Bucket> buckets = new HashMap<String, Bucket>();
  
  @Override
  public String getSource() { return null; }

  @Override
  public void inform(SolrCore core) {
    this.core = core;
    SolrQueryRequest req = new SolrQueryRequestBase(core, new SolrParams() {
      
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
    UpdateRequestProcessorChain processorChain =
        core.getUpdateProcessingChain("");
    processor = processorChain.createProcessor(req, rsp);
    zkClient = getZkClient();
  }
  
  @Override
  public void init(NamedList args) {
    super.init(args);
    Map<String,Object> params = toMap((NamedList<String>)args.get(CommonConstants.HANDLER_PARAMS));
    username = params.get(CommonConstants.USERNAME_FIELD).toString();
    password = params.get(CommonConstants.PASSWORD_FIELD).toString();
    port = (int)params.get(CommonConstants.PORT_FIELD);
    commitAfterBatch = (boolean)params.get(CommonConstants.COMMIT_AFTER_BATCH_FIELD);
    couchbaseServersList = new ArrayList<String>(SolrParams.toMap((NamedList)params.get(CommonConstants.COUCHBASE_SERVERS_MARK)).values());
    if(couchbaseServersList == null) {
      LOG.error("No Couchbase servers configured!");
    }
    clusterName = (String) params.get(CommonConstants.CLUSTER_NAME_MARK);
    if(clusterName == null) {
      clusterName = "default";
    }
    List<NamedList<Object>> bucketslist = args.getAll(CommonConstants.BUCKET_MARK);
    for(NamedList<Object> bucket : bucketslist) {
      String name = (String)bucket.get(CommonConstants.NAME_FIELD);
      String splitpath = (String)bucket.get(CommonConstants.SPLITPATH_FIELD);
      NamedList<Object> mappingslist = (NamedList<Object>) bucket.get(CommonConstants.FIELD_MAPPING_FIELD);
      Map<String,String> fieldmappings = SolrParams.toMap(mappingslist);
      Bucket b = new Bucket(name, splitpath, fieldmappings);
      buckets.put(name, b);
    }
    //configure Couchbase REST client
    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    for(String host : couchbaseServersList) {
      credsProvider.setCredentials(new AuthScope(null, -1, null),
          new UsernamePasswordCredentials(username, password));
    }
    httpClient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
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
    if(core != null) {
      CoreContainer container = core.getCoreDescriptor().getCoreContainer();
      if(container.isZooKeeperAware()) {
        client = container.getZkController().getZkClient();
      }
    }
    return client;
  }
  public boolean isSolrCloud() {
    return core.getCoreDescriptor().getCoreContainer().isZooKeeperAware();
  }
  
  public void handleStart() {
    //Check if in SolrCloud mode
    if(isSolrCloud()) {
      checkIfIamLeader();
    } else {
      startCouchbaseReplica();
    }
  }
  
  public void handleStop() {
    stopCouchbaseReplica();
  }
  
  public void checkIfIamLeader() {
    //at first, check if I am the first shard leader, which shoudld start Couchbase replica
    zkStateReader = new ZkStateReader(getZkClient());
    try {
      zkStateReader.updateClusterState(true);
    } catch (KeeperException | InterruptedException e1) {
      LOG.error("Error while updating Cluster State!", e1);
    }
    String collection = core.getName();
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
    final String coreNodeName = core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName();
    if(replica != null && coreNodeName != null && replica.getName().equals(coreNodeName)) { // I am the leader
      startCouchbaseReplica();
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
  
  public static int checkPort(int port) {
    ServerSocket s = null;
    for(;;) {
      try {
        s = new ServerSocket(port);
        s.close();
        break;
      } catch (IOException e) {
        //port occupied, find another one
        port++;
      }
    }
    
    return port;
  }
  
  public void startCouchbaseReplica() {
    settings = new Settings();
    this.documentTypeParentFields = settings.getByPrefix("couchbase.documentTypeParentFields.");
    for (String key: documentTypeParentFields.keySet()) {
        String parentField = documentTypeParentFields.get(key);
        LOG.info("Using field {} as parent for type {}", parentField, key);
    }

    this.documentTypeRoutingFields = settings.getByPrefix("couchbase.documentTypeRoutingFields.");
    for (String key: documentTypeRoutingFields.keySet()) {
        String routingField = documentTypeRoutingFields.get(key);
        LOG.info("Using field {} as routing for type {}", routingField, key);
    }
    typeSelector = new DefaultTypeSelector();
    typeSelector.configure(settings);
    couchbaseBehaviour = new SolrCouchbaseBehaviour(this);
    port = checkPort(port);
    capiBehaviour = new SolrCAPIBehaviour(this, typeSelector, documentTypeParentFields, documentTypeRoutingFields, commitAfterBatch);
    server = new CAPIServer(capiBehaviour, couchbaseBehaviour, port, username, password);
    //TODO fix this
    try{
      server.start();
    } catch (Exception e) {
      LOG.error("Could not start CAPIServer!", e);
    }
    port = server.getPort();
    LOG.info(String.format("CAPIServer started on port %d", port));
  }
  
  public void stopCouchbaseReplica() {
    if(server != null) {
      try {
        server.stop();
      } catch (Exception e) {
        LOG.error("Error while stopping Couchbase server.", e);
      }
    }
  }
  
  public UpdateRequestProcessor getProcessor() {
    return this.processor;
  }
  
  public SolrCore getCore() {
    return this.core;
  }
  
  public Map<String,Bucket> getBuckets() {
    return buckets;
  }
  
  public Bucket getBucket(String name) {
    return buckets.get(name);
  }
  
  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }
  
  /** Create a Map&lt;String,Object&gt; from a NamedList given no keys are repeated */
  public static Map<String,Object> toMap(NamedList params) {
    HashMap<String,Object> map = new HashMap<>();
    for (int i=0; i<params.size(); i++) {
      map.put(params.getName(i), params.getVal(i));
    }
    return map;
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
