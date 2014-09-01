package com.lucidworks.couchbase;
import java.util.Map;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.capi.CAPIBehavior;
import com.couchbase.capi.CAPIServer;
import com.couchbase.capi.CouchbaseBehavior;


public class CouchbaseRequestHandler extends RequestHandlerBase {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseRequestHandler.class);

  CouchbaseBehavior couchbaseBehaviour;
  CAPIBehavior capiBehaviour;
  CAPIServer server;
  int port = 9876;
  String username = "admin";
  String password = "admin123";
  private TypeSelector typeSelector;
  private Settings settings;

  private Map<String, String> documentTypeParentFields;
  private Map<String, String> documentTypeRoutingFields;
  
  @Override
  public void init(NamedList args) {
    super.init(args);
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
    couchbaseBehaviour = new SolrCouchbaseBehaviour();
    capiBehaviour = new SolrCAPIBehaviour(typeSelector, documentTypeParentFields, documentTypeRoutingFields);
    
  }
  
  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
      throws Exception {
    SolrParams params = req.getParams();
    String q = params.get(CommonParams.Q);
    
    q = q.toLowerCase();
    q = q.trim();
    
    switch(q) {
    case "start"  :
      startCouchbasePlugin();
      break;
    case "stop" :
      stopCouchbasePlugin();
      break;
    }
  }

  @Override
  public String getDescription() {
    return "Couchbase plugin";
  }
  
  @Override
  public String getSource() { return null; }

  public void startCouchbasePlugin() {
    server = new CAPIServer(capiBehaviour, couchbaseBehaviour, port, username, password);
    //TODO fix this
    try{
      server.start();
    } catch (Exception e) {
      
    }
    port = server.getPort();
//    LOG.info(String.format("CAPIServer started on port %d", port));
  }
  
  public void stopCouchbasePlugin() {
    
  }
}
