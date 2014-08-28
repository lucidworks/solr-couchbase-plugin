package com.lucidworks.couchbase;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import com.couchbase.capi.CAPIBehavior;
import com.couchbase.capi.CAPIServer;
import com.couchbase.capi.CouchbaseBehavior;


public class CouchbaseRequestHandler extends RequestHandlerBase {

//  protected static final Logger logger = LoggerFactory.getLogger(CAPITestCase.class);
  CouchbaseBehavior couchbaseBehaviour;
  CAPIBehavior capiBehaviour;
  CAPIServer server;
  int port = 9876;
  String username = "admin";
  String password = "admin123";
  
  @Override
  public void init(NamedList args) {
    super.init(args);
    couchbaseBehaviour = new SolrCouchbaseBehaviour();
    capiBehaviour = new SolrCAPIBehaviour();
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
//    logger.info(String.format("CAPIServer started on port %d", port));
  }
  
  public void stopCouchbasePlugin() {
    
  }
}
