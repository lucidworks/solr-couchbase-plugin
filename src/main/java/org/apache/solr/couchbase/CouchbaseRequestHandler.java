package org.apache.solr.couchbase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.cloud.ElectionContext;
import org.apache.solr.cloud.LeaderElector;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.capi.CAPIBehavior;
import com.couchbase.capi.CAPIServer;
import com.couchbase.capi.CouchbaseBehavior;


public class CouchbaseRequestHandler extends RequestHandlerBase implements SolrCoreAware {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseRequestHandler.class);
  private static final String SKIP_INIT = "skip-init";
  
  private CouchbaseBehavior couchbaseBehaviour;
  private CAPIBehavior capiBehaviour;
  private CAPIServer server;
  private String host = "127.0.0.1";
  private int port;
  private String username;
  private String password;
  private TypeSelector typeSelector;
  private Settings settings;
  private SolrCore core;
  private UpdateRequestProcessor processor;
  private LeaderElector elector;
  private ElectionContext context;
  private static SolrZkClient zkClient;
  private static String zkServers = "";
  private ZkStateReader zkStateReader;
  private boolean commitAfterBatch;

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
    Map<String,String> params = SolrParams.toMap((NamedList<String>)args.get(CommonConstants.HANDLER_PARAMS));
    username = params.get(CommonConstants.USERNAME_FIELD);
    password = params.get(CommonConstants.PASSWORD_FIELD);
    port = Integer.parseInt(params.get(CommonConstants.PORT_FIELD));
    commitAfterBatch = Boolean.parseBoolean(params.get(CommonConstants.COMMIT_AFTER_BATCH_FIELD));
    
    List<NamedList<Object>> bucketslist = args.getAll(CommonConstants.BUCKET_MARK);
    for(NamedList<Object> bucket : bucketslist) {
      String name = (String)bucket.get(CommonConstants.NAME_FIELD);
      String splitpath = (String)bucket.get(CommonConstants.SPLITPATH_FIELD);
      NamedList<Object> mappingslist = (NamedList<Object>) bucket.get(CommonConstants.FIELD_MAPPING_FIELD);
      Map<String,String> fieldmappings = SolrParams.toMap(mappingslist);
      Bucket b = new Bucket(name, splitpath, fieldmappings);
      buckets.put(name, b);
    }
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
    stopCouchbasePlugin();
  }
  
  public void checkIfIamLeader() {
    //at first, check if I am the first leader, which shoudld start Couchbase replica
    zkStateReader = new ZkStateReader(getZkClient());
    try {
      zkStateReader.updateClusterState(true);
    } catch (KeeperException | InterruptedException e1) {
      LOG.error("Error while updating Cluster State!", e1);
    }
    String collection = "collection1";
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
    //if I am leader
    if(replica != null && coreNodeName != null && replica.getName().equals(coreNodeName)) {
      startCouchbaseReplica();
    }
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
  
  public void stopCouchbasePlugin() {
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
}
