package org.apache.solr.couchbase;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClients;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

@Ignore
public class CloudTest2 extends LuceneTestCase {

  private static Logger LOG = LoggerFactory.getLogger(MiniSolrCloudCluster.class);
  private static final int NUM_SERVERS = 2;
  private static final int NUM_SHARDS = 2;
  private static final int REPLICATION_FACTOR = 1;
  private static final int TIMEOUT = 10000;
  private static final String PLUGIN_URI = "/couchbase";
  private static final String username = System.getProperty("cb.username");
  private static final String password = System.getProperty("cb.password");
  private static final String TEST_DIR = System.getProperty("test.dir");
  
  
  protected static MiniSolrCloudCluster miniCluster;
  protected static SolrZkClient zkClient;
  protected static HttpClient httpClient;
  
  @Rule
  public TestRule solrTestRules = RuleChain
      .outerRule(new SystemPropertiesRestoreRule());
  
  @ClassRule
  public static TestRule solrClassRules = RuleChain.outerRule(
      new SystemPropertiesRestoreRule()).around(
      new RevertDefaultThreadHandlerRule());
  
  @BeforeClass
  public static void startup() throws Exception {
    miniCluster = new MiniSolrCloudCluster(NUM_SERVERS, null, new File(TEST_DIR + File.separator + "solr", "solr.xml"),
      null, null);
    zkClient = getZkClient();
    httpClient = getHttpClient();
  }
  
  @AfterClass
  public static void shutdown() throws Exception {
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
    miniCluster = null;
    if(zkClient != null) {
      zkClient.close();
    }
    zkClient = null;
    httpClient = null;
    System.clearProperty("solr.tests.mergePolicy");
    System.clearProperty("solr.tests.maxBufferedDocs");
    System.clearProperty("solr.tests.maxIndexingThreads");
    System.clearProperty("solr.tests.ramBufferSizeMB");
    System.clearProperty("solr.tests.mergeScheduler");
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.solrxml.location");
    System.clearProperty("zkHost");
  }
  
  @Test
  public void testReplication() {
    assertNotNull(miniCluster.getZkServer());
    List<JettySolrRunner> jettys = miniCluster.getJettySolrRunners();
    assertEquals(NUM_SERVERS, jettys.size());
    for (JettySolrRunner jetty : jettys) {
      assertTrue(jetty.isRunning());
    }
    int port = miniCluster.getZkServer().getPort();
    CloudSolrServer cloudSolrServer = null;
    try {
      cloudSolrServer = new CloudSolrServer(miniCluster.getZkServer().getZkAddress(), true);
      cloudSolrServer.connect();

      // create collection
      String collectionName = "testCollection";
      String configName = "solrconfig-empty";
      System.setProperty("solr.tests.mergePolicy", "org.apache.lucene.index.TieredMergePolicy");
      String confDir = TEST_DIR + File.separator + "solr" + File.separator + "collection1" + File.separator + "conf";
      uploadConfigToZk(confDir, configName);
      createCollection(cloudSolrServer, collectionName, NUM_SHARDS, REPLICATION_FACTOR, configName);
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

      
    //Check Solr nodes and activate plugins
//    for(String collection : collectionNames) {
//      for (JettySolrRunner jetty : jettys) {
//        assertTrue(jetty.isRunning());
//        solrPorts.add(jetty.getLocalPort());
//        URL url = jetty.getBaseUrl();
//        boolean success = activateSolrPlugin(jetty, collection);
//        assertTrue(success);
//      }
//    }
  }
  
  protected void uploadConfigToZk(String configDir, String configName) throws Exception {
    // override settings in the solrconfig include
    System.setProperty("solr.tests.maxBufferedDocs", "100000");
    System.setProperty("solr.tests.maxIndexingThreads", "-1");
    System.setProperty("solr.tests.ramBufferSizeMB", "100");
    // use non-test classes so RandomizedRunner isn't necessary
    System.setProperty("solr.tests.mergeScheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
    System.setProperty("solr.directoryFactory", "solr.RAMDirectoryFactory");
    zkClient =  new SolrZkClient(miniCluster.getZkServer().getZkAddress(), TIMEOUT, 45000, null);
    IOFileFilter dirfilter = new IOFileFilter() {
      
      @Override
      public boolean accept(File dir, String name) {
        return accept(dir);
      }
      
      @Override
      public boolean accept(File file) {
        if(file.isDirectory() && (file.getName().equals("lang") || file.getName().equals("conf"))) {
          return true;
        } else {
          return false;
        }
      }
    };
    
    IOFileFilter fileFilter = new IOFileFilter() {
      
      @Override
      public boolean accept(File dir, String name) {
        // TODO Auto-generated method stub
        return true;
      }
      
      @Override
      public boolean accept(File file) {
        // TODO Auto-generated method stub
        return true;
      }
    };
    Collection<File> confFiles = FileUtils.listFiles(new File(configDir), fileFilter, dirfilter);
    for(File f : confFiles) {
      uploadConfigFileToZk(zkClient, configName, f.getName(), f);
    }
  }
  
  protected void uploadConfigFileToZk(SolrZkClient zkClient, String configName, String nameInZk, File file)
      throws Exception {
    zkClient.makePath(ZkController.CONFIGS_ZKNODE + "/" + configName + "/" + nameInZk, file, false, true);
  }
  
  protected NamedList<Object> createCollection(CloudSolrServer server, String name, int numShards,
      int replicationFactor, String configName) throws Exception {
    ModifiableSolrParams modParams = new ModifiableSolrParams();
    modParams.set(CoreAdminParams.ACTION, CollectionAction.CREATE.name());
    modParams.set("name", name);
    modParams.set("numShards", numShards);
    modParams.set("replicationFactor", replicationFactor);
    modParams.set("collection.configName", configName);
    QueryRequest request = new QueryRequest(modParams);
    request.setPath("/admin/collections");
    return server.request(request);
  }
  
  public static HttpClient getHttpClient() {
   //configure Couchbase REST client
    CredentialsProvider credsProvider = new BasicCredentialsProvider();
    credsProvider.setCredentials(new AuthScope(null, -1, null),
        new UsernamePasswordCredentials(username, password));
    return HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
  }
  
  public static SolrZkClient getZkClient() {
    return new SolrZkClient(miniCluster.getZkServer().getZkHost(),
        10000, 45000, null);
  }
  
  public boolean activateSolrPlugin(JettySolrRunner jetty, String collection) {
    CloseableHttpResponse response = null;
    boolean result = false;
    ArrayList<Map<String,Object>> responseArray = null;
    try {
      URI plugin = new URIBuilder().setScheme("http")
          .setHost("127.0.0.1")
          .setPort(jetty.getLocalPort())
          .setPath("/solr/" + collection + PLUGIN_URI)
          .setParameter("action", "start")
          .build();
      HttpGet startPlugin = new HttpGet(plugin);
      response = (CloseableHttpResponse) getHttpClient().execute(startPlugin);
      int code = response.getStatusLine().getStatusCode();
      if(code >= 200 && code < 300) {
       result = true;
      }
    } catch (URISyntaxException e) {
      LOG.error("Could not create Couchbase clusters URI", e);
    } catch (ClientProtocolException e) {
      LOG.error("Client Protocol Exception", e);
    } catch (IOException e) {
      LOG.error("IOException", e);
    } finally {
      try {
        if(response != null) {
          response.close();
        }
      } catch (IOException e) {
        LOG.error("Could not close HTTP response", e);
      }
    }
    return result;
  }
}
