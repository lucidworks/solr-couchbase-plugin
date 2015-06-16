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
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
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
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
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
    this.numVBuckets = (int)params.get(CommonConstants.NUM_VBUCKETS_FIELD);
    this.clusterName = (params.get(CommonConstants.COUCHBASE_CLUSTER_NAME_FIELD) == null) ? "cluster-" + System.currentTimeMillis() :
      String.valueOf(params.get(CommonConstants.COUCHBASE_CLUSTER_NAME_FIELD));
    this.commitAfterBatch = (boolean)params.get(CommonConstants.COMMIT_AFTER_BATCH_FIELD);
    this.buckets = new HashMap<String, Bucket>();
    this.replications = new ArrayList<String>();
    
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
      vb_num = getNumVBuckets();
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
      if(requestHandler.getZkClient() != null) {
        createZKCapiServer();
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
  
  public void createZKCapiServer() {
    Map<String,Object> properties = new HashMap<String, Object>();
    properties.put("core", requestHandler.getCore().getName());
    properties.put("node_name", "127.0.0.1:" + clientPort + "_capiserver");
    properties.put("host", "localhost");
    properties.put("port", clientPort);
    
    ZkNodeProps nodeProps = new ZkNodeProps(properties);
    try {
      requestHandler.getZkClient().makePath(CouchbaseRequestHandler.CAPISERVER_PATH + "/" + properties.get("node_name"), ZkStateReader.toJSON(nodeProps),
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

  //Creating destination cluster reference
  public boolean createRemoteCluster(String uuid) {
    MultivaluedMap formData = new MultivaluedHashMap();
    formData.add("uuid", uuid);
    formData.add("name", clusterName);
    formData.add("hostname", clientHost + ":" + clientPort);
    formData.add("username", clientUsername);
    formData.add("password", clientPassword);

    Response response = couchbaseService.path("pools")
        .path("default")
        .path("remoteClusters")
        .request()
        .post(Entity.entity(formData, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
    // TODO XXX Use Jackson
    JSONObject entity = response.readEntity(JSONObject.class);
    if(response.getStatus() == 200) {
      try {
        remoteClusterUuid = entity.getString("uuid");
      } catch (JSONException e) {
        LOG.error("Could not parse response!", e);
      }
      LOG.info("Remote cluter = " + clusterName + " created.");
      return true;
    } else {
      LOG.warn("Remote cluster=" + clusterName + " creation failed with message: " + entity.toString());
    }
    return false;
  }

  public boolean deleteRemoteCluster() {
    boolean exists = false;
    boolean success = false;
    if(remoteClusterUuid != null) {
      exists = checkRemoteClusterExists(remoteClusterUuid);
    } else {
      exists = checkRemoteClusterExists(getRemoteClusterUuid());
    }
    if(exists) {

  
      Response response = couchbaseService.path("pools")
          .path("default").path("remoteClusters").path(clusterName).request().delete();
      response.close(); // close right away; we only care about the status code.
      if(response.getStatus() == 200) {
        remoteClusterUuid = null;
        LOG.info("DELETED remote cluster name=" + clusterName);
        success = true;
      } else {
        LOG.error("Error while deleting remote cluster name=" + clusterName);
      }
    } else {
      LOG.error("Cluster `" + clusterName + "` does not exist in the Couchbase server");
    }
    return success;
  }
  
  public int getNumVBuckets() throws Exception {
    int vbuckets = -1;
    JSONObject obj;
    
    try {
      obj = new JSONObject(couchbaseService.path("pools").path("default").path("buckets").path("default")
          .path("nodes").path(serverHost + "%3A" + String.valueOf(serverPort)).path("stats")
          .request(MediaType.APPLICATION_JSON).get(String.class));
      Object o = obj.getJSONObject("op").getJSONObject("samples").getJSONArray("ep_vb_total").get(0);
      if(o instanceof Integer) {
        int value = (Integer)o;
        if(value == 64 || value == 1024) {
          vbuckets = value;
        }
      }
    } catch (Exception e) {
      try {
        obj = new JSONObject(couchbaseService.path("pools").path("default")
            .request(MediaType.APPLICATION_JSON).get(String.class));
        obj = obj.getJSONArray("nodes").getJSONObject(0);
        String os = obj.getString("os");
        if(os.contains("apple")) {
          vbuckets = 64;
        } else {
          vbuckets = 1024;
        }
      } catch (Exception e1) {
        throw new Exception("Couchbase connector cannot obtain NUM_VBUCKETS value from the Couchbase server.", e1);
      }
    }
    return vbuckets;
  }

  public String getRemoteClusterUuid() {
    String url = "http://" + getClientHost() + ":" + clientPort;
    WebTarget remoteServerService = client.target(UriBuilder.fromUri(url).build());

    String  uuid = null;

    try {
      JSONObject obj = new JSONObject(remoteServerService.path("pools")
          .path("default").path("remoteCluster").request(MediaType.APPLICATION_JSON).get(String.class));
      obj = obj.getJSONObject("buckets");
      String uri = obj.getString("uri");
      uuid = uri.substring(uri.indexOf("uuid=")+"uuid=".length());
    }
    catch(Exception e) {
      LOG.warn("Getting remote cluster UUID failed. " + e.getMessage());
    }
    return uuid;
  }

  public boolean checkRemoteClusterExists(String uuid) {
    try {
      JSONArray obj = new JSONArray(couchbaseService.path("pools")
          .path("default").path("remoteClusters").request(MediaType.APPLICATION_JSON).get(String.class));
      for(int i = 0; i < obj.length(); i++) {
        JSONObject cluster = (JSONObject)obj.get(i);
        String clusterUuid = cluster.getString("uuid");
        if(uuid.equals(clusterUuid)) {
          boolean deleted = cluster.getBoolean("deleted");
          if(!deleted) {
            return true;
          }
        }
      }
    } catch(Exception e) {
      LOG.warn("Failed to check if remote cluster exists. " + e.getMessage());
    } 
    return false;
  }

  //Create XDCR replications
  public List<String> createReplication(String uuid) throws Exception {
    List<String> ids = new ArrayList<String>();

    for(Bucket bucket : buckets.values()) {
      MultivaluedMap formData = new MultivaluedHashMap();
      formData.add("uuid", uuid);
      formData.add("toCluster", clusterName);
      formData.add("fromBucket", bucket.getName());
      formData.add("toBucket", bucket.getName());
      formData.add("replicationType", "continuous");
      formData.add("type", "capi");

      Response response = couchbaseService.path("controller")
          .path("createReplication")
          .request()
          .post(Entity.entity(formData, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
      JSONObject entity = response.readEntity(JSONObject.class);
      if(response.getStatus() == 200) {
        String id = entity.getString("id");
        replications.add(id);
        ids.add(id);
        LOG.info("Created Couchbase replication from bucket = " + bucket.getName() + " to client bucket = " + collection);
      } else {
        try {
          throw new Exception("Failed to create Couchbase replication from Couchbase bucket = " + bucket.getName() + ". Errors:\n" + entity.getString("errors"));
        } catch (JSONException e) {
          throw new Exception("Failed to create Couchbase replication from Couchbase bucket = " + bucket.getName() + ".");
        }
      }
    }
    return ids;
  }
 
  // Create XDCR replications
  public boolean createReplication(String uuid, String clusterName, String fromBucket, String toBucket) {
    boolean success = false;
    MultivaluedMap formData = new MultivaluedHashMap();
    formData.add("uuid", uuid);
    formData.add("toCluster", clusterName);
    formData.add("fromBucket", fromBucket);
    formData.add("toBucket", toBucket);
    formData.add("replicationType", "continuous");
    formData.add("type", "capi");

    Response response = couchbaseService.path("controller")
        .path("createReplication")
        .request()
        .post(Entity.entity(formData, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
    response.close(); // close right away; we're not paying attention to entity
    if(response.getStatus() == 200) {
      LOG.debug("Created Couchbase replication from Couchbase bucket = " + fromBucket + " to Couchbase client bucket = " + toBucket);
      success = true;
    } else {
      LOG.warn("Failed to create Couchbase replication from Couchbase bucket = " + fromBucket+ " to Couchbase client bucket = " + toBucket);
    }
    return success;
  }
  
  public boolean deleteReplication(String id) {
    boolean success = false;

    String fixedId = id.replace("/", "%2F");
    Response response = couchbaseService.path("controller")
        .path("cancelXDCR").path(fixedId).request().delete();
//    JSONObject entity = response.getEntity(JSONObject.class);
    response.close(); // close right away; we're not paying attention to entity
    if(response.getStatus() == 200) {
      replications.remove(id);
      success = true;
      LOG.info("DELETED replication id=" + id);
    } else {
      LOG.error("Error while deleting replication id=" + id + ". " + response.toString());
    }
    return success;
  }
  
  public void close() throws Exception {
    if(running) {
      List<String> replicationsIds = new ArrayList<String>(replications);
      for(String id : replicationsIds) {
        deleteReplication(id);
      }
      deleteRemoteCluster();
      stopCouchbaseReplica();
    }
  }
}
