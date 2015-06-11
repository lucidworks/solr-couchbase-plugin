package org.apache.solr.couchbase;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.UriBuilder;

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.couchbase.capi.SolrCAPIBehaviour;
import org.apache.solr.couchbase.capi.SolrCouchbaseBehaviour;
import org.apache.solr.couchbase.common.Bucket;
import org.apache.solr.couchbase.common.CommonConstants;
import org.apache.solr.couchbase.common.DefaultTypeSelector;
import org.apache.solr.couchbase.common.Settings;
import org.apache.solr.couchbase.common.TypeSelector;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.capi.CAPIBehavior;
import com.couchbase.capi.CAPIServer;
import com.couchbase.capi.CouchbaseBehavior;

public class CouchbaseReplica {
  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseReplica.class);
  
  private CouchbaseRequestHandler requestHandler;
  private CouchbaseBehavior couchbaseBehaviour;
  private CAPIBehavior capiBehaviour;
  private CAPIServer server;
  
  private final String serverHost;
  private final int serverPort;
  private String clientHost;
  private int clientPort = -1;
  private final String clientUsername, serverUsername;
  private final String clientPassword, serverPassword;
  private int numVBuckets;
  private String clusterName;
  private String remoteClusterUuid;
  private TypeSelector typeSelector;
  private Settings settings;
  private boolean commitAfterBatch;
  private String collection;
  private boolean running = false;
  
  private Map<String, String> documentTypeParentFields;
  private Map<String, String> documentTypeRoutingFields;
  private Map<String,Bucket> buckets;
  private List<String> replications;

  private final Client client;
  private final WebTarget couchbaseService;
  
  public CouchbaseReplica(CouchbaseRequestHandler handler, Map<String,Object> params, List<NamedList<Object>> bucketsList) {
    this.requestHandler = handler;
    this.serverHost = (params.get(CommonConstants.HOST_IP) == null) ? "127.0.0.1" : String.valueOf(params.get(CommonConstants.HOST_IP));
    this.serverPort = 8091;
    this.clientHost = "127.0.0.1";
    this.clientPort = (int)params.get(CommonConstants.PORT_FIELD);
    this.serverUsername = String.valueOf(params.get(CommonConstants.USERNAME_FIELD));
    this.serverPassword = String.valueOf(params.get(CommonConstants.PASSWORD_FIELD));
    this.clientUsername = this.serverUsername;
    this.clientPassword = this.serverPassword;
    numVBuckets = (int)params.get(CommonConstants.NUM_VBUCKETS_FIELD);
    commitAfterBatch = (boolean)params.get(CommonConstants.COMMIT_AFTER_BATCH_FIELD);
    
    for(NamedList<Object> bucket : bucketsList) {
      String name = (String)bucket.get(CommonConstants.NAME_FIELD);
      String splitpath = (String)bucket.get(CommonConstants.SPLITPATH_FIELD);
      NamedList<Object> mappingslist = (NamedList<Object>) bucket.get(CommonConstants.FIELD_MAPPING_FIELD);
      Map<String,String> fieldmappings = SolrParams.toMap(mappingslist);
      Bucket b = new Bucket(name, splitpath, fieldmappings);
      buckets.put(name, b);
    }
    
    this.buckets = new HashMap<String, Bucket>();
    this.replications = new ArrayList<String>();
    
    this.client = createJerseyClient();
    this.couchbaseService = createCouchbaseServiceTarget();
  }

  private Client createJerseyClient() {
    ClientConfig clientConfig = new ClientConfig();
    Client client = ClientBuilder.newClient(clientConfig);
    HttpAuthenticationFeature feature = HttpAuthenticationFeature.basic(serverUsername, serverPassword);
    client.register(feature);
    return client;
  }

  private WebTarget createCouchbaseServiceTarget() {
    String url = "http://" + serverHost + ":" + serverPort;
    return client.target(UriBuilder.fromUri(url).build());
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
    clientPort = checkPort(clientPort);
    capiBehaviour = new SolrCAPIBehaviour(this, typeSelector, documentTypeParentFields, documentTypeRoutingFields, commitAfterBatch);
    int vb_num;
    try {
      vb_num = CouchbaseUtils.getNumVBuckets();
      if(vb_num == 64 || vb_num == 1024) {
        numVBuckets = vb_num;
      }
    } catch (Exception e) {
      LOG.info("Couchbase connector cannot obtain numVBuckets parameter from Couchbase server. Input value will be used.", e);
    }
    server = new CAPIServer(capiBehaviour, couchbaseBehaviour, new InetSocketAddress("0.0.0.0", clientPort), clientUsername, clientPassword, numVBuckets);
    try{
      server.start();
      clientPort = server.getPort();
      LOG.info(String.format("CAPIServer started on port %d", clientPort));
//      configureXDCR();
      SolrZkClient zkClient = requestHandler.getZkClient();
      if(zkClient != null) {
        createZKCapiServer(zkClient);
      }
      running = true;
    } catch (Exception e) {
      LOG.error("Could not start CAPIServer!", e);
    }
  }
  
  public void stopCouchbaseReplica() {
    if(server != null) {
      try {
        server.stop();
        running = false;
      } catch (Exception e) {
        LOG.error("Error while stopping Couchbase server.", e);
      }
    }
  }
  
  public void createZKCapiServer(SolrZkClient zkClient) {
    Map<String,Object> properties = new HashMap<String, Object>();
    properties.put("core", collection);
    properties.put("node_name", "127.0.0.1:" + clientPort + "_capiserver");
    properties.put("host", "localhost");
    properties.put("port", clientPort);
    
    ZkNodeProps nodeProps = new ZkNodeProps(properties);
    try {
      zkClient.makePath(CouchbaseRequestHandler.CAPISERVER_PATH + "/" + properties.get("node_name"), ZkStateReader.toJSON(nodeProps),
          CreateMode.EPHEMERAL, true);
    } catch (KeeperException | InterruptedException e) {
      LOG.error("CAPIServer could not create ephemeral node in ZooKeeper!", e);
    }
  }
  
  public static int checkPort(int port) {
    ServerSocket s = null;
    int result = -1;
    for(;;) {
      try {
        s = new ServerSocket(port);
        result = port;
        s.close();
        break;
      } catch (IOException e) {
        //port occupied, find another one
        try {
          s = new ServerSocket(0);
          result = s.getLocalPort();
          s.close();
        } catch (IOException e1) {
          LOG.error("Couchbase plugin could not find a free port to start a Couchbase replica", e);
        }
      }
    }
    
    return result;
  }

  public CouchbaseRequestHandler getRequestHandler() {
    return requestHandler;
  }

  public String getServerHost() {
    return serverHost;
  }

  public int getServerPort() {
    return serverPort;
  }

  public String getClientHost() {
    return clientHost;
  }

  public int getClientPort() {
    return clientPort;
  }

  public boolean isRunning() {
    return running;
  }
  
  public Map<String,Bucket> getBuckets() {
    return buckets;
  }
  
  public Bucket getBucket(String name) {
    return buckets.get(name);
  }
}
