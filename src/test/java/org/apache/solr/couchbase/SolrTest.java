package org.apache.solr.couchbase;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.junit.BeforeClass;

public class SolrTest extends SolrTestCaseJ4 {

  public static final String SOLRCONFIG_PATH = "";
  public static final String SCHEMA_PATH = "";
  EmbeddedSolrServer server;
  CoreContainer container;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore(SOLRCONFIG_PATH, SCHEMA_PATH);
  }
  
  public void testSingleSolrReplication() {
    
  }
}
