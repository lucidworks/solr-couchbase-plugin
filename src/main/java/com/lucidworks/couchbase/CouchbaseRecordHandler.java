package com.lucidworks.couchbase;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.JsonRecordReader.Handler;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchbaseRecordHandler implements Handler{
  
  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseRequestHandler.class);
  
  SolrCAPIBehaviour capiBehaviour;
  SolrInputDocument doc;
  SolrQueryRequest req;
  
  public CouchbaseRecordHandler(SolrCAPIBehaviour capiBehaviour, SolrQueryRequest req, SolrInputDocument doc) {
    this.capiBehaviour = capiBehaviour;
    this.doc = doc;
    this.req = req;
  } 
  
  @Override
  public void handle(Map<String, Object> record, String path) {
    for(Map.Entry<String, Object> entry : record.entrySet()) {
      if(entry.getKey().equals("last_modified")) {
        DateFormat formatter = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
        Date date = null;
        try {
          date = (Date)formatter.parse((String) entry.getValue());
        } catch (ParseException e) {
          LOG.error("Solr Couchbase plugin could not parse date", e);
        }
        doc.setField(entry.getKey(), date);
      } else {
        doc.addField(entry.getKey(), entry.getValue());
      }
    }
    if((boolean)doc.getFieldValue(SolrCAPIBehaviour.DELETED_FIELD)) {
      capiBehaviour.deleteDoc(doc.getFieldValue(SolrCAPIBehaviour.ID_FIELD), req);
    } else {
      capiBehaviour.addDoc(doc, req);
    }
  }

}
