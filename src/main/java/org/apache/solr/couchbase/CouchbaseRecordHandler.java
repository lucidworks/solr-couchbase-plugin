package org.apache.solr.couchbase;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.JsonRecordReader.Handler;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CouchbaseRecordHandler implements Handler{
  
  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseRequestHandler.class);
  
  List<Object> bulkDocsResult = new ArrayList<Object>();
  Map<String,String> revisions = new HashMap<String, String>();
  SolrCAPIBehaviour capiBehaviour;
  SolrInputDocument doc;
  SolrQueryRequest req;
  int seq = 1;
  
  public CouchbaseRecordHandler(SolrCAPIBehaviour capiBehaviour, SolrQueryRequest req, SolrInputDocument doc, List<Object> bulkDocsResult) {
    this.capiBehaviour = capiBehaviour;
    this.doc = doc;
    this.req = req;
    this.bulkDocsResult = bulkDocsResult;
  } 
  
  @Override
  public void handle(Map<String, Object> record, String path) {
    SolrInputDocument solrDoc = doc.deepCopy();
    Map<String,String> mapping = SolrUtils.mapToSolrDynamicFields(record);
    if(!path.equals("/")) {
      solrDoc.setField(CommonConstants.ID_FIELD, (String)doc.getFieldValue(CommonConstants.ID_FIELD) + "-" + seq);
      seq++;
    }
    for(Map.Entry<String, Object> entry : record.entrySet()) {
      String key = entry.getKey();
      if(mapping.containsKey(key)) {
        key = mapping.get(key);
      }
      if(entry.getKey().equals("last_modified")) {
        DateFormat formatter = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
        Date date = null;
        try {
          date = (Date)formatter.parse((String) entry.getValue());
        } catch (ParseException e) {
          LOG.error("Solr Couchbase plugin could not parse date", e);
        }
        solrDoc.setField(key, date);
      } else {
        solrDoc.addField(key, entry.getValue());
      }
    }
    
    boolean success = false;
    if((boolean)doc.getFieldValue(CommonConstants.DELETED_FIELD)) {
      success = capiBehaviour.deleteDoc(solrDoc.getFieldValue(CommonConstants.ID_FIELD), req);
    } else {
      success = capiBehaviour.addDoc(solrDoc, req);
    }

    String itemId = (String) doc.getFieldValue(CommonConstants.ID_FIELD);
    String itemRev = (String) doc.getFieldValue(CommonConstants.REVISION_FIELD);
    Map<String, Object> itemResponse = new HashMap<String, Object>();
    itemResponse.put("id", itemId);
    itemResponse.put("rev", itemRev);
    
    if(success) {
      if(!itemRev.equals(revisions.get(itemId))) {
        revisions.put(itemId, itemRev);
        bulkDocsResult.add(itemResponse);
      }
    }
  }

}
