package org.apache.solr.couchbase.capi;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.JsonRecordReader;
import org.apache.solr.common.util.JsonRecordReader.Handler;
import org.apache.solr.couchbase.CouchbaseRecordHandler;
import org.apache.solr.couchbase.CouchbaseReplica;
import org.apache.solr.couchbase.CouchbaseRequestHandler;
import org.apache.solr.couchbase.common.CommonConstants;
import org.apache.solr.couchbase.common.Counter;
import org.apache.solr.couchbase.common.TypeSelector;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.RefCounted;
import org.codehaus.jackson.map.ObjectMapper;
import org.noggit.JSONParser;
import org.noggit.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.capi.CAPIBehavior;

public class SolrCAPIBehaviour implements CAPIBehavior {
  
  private static final Logger LOG = LoggerFactory.getLogger(SolrCAPIBehaviour.class);
  
  protected ObjectMapper mapper = new ObjectMapper();
  private TypeSelector typeSelector;
  protected Map<String, String> documentTypeParentFields;
  protected Map<String, String> documentTypeRoutingFields;
  protected CouchbaseReplica couchbase;
  protected CouchbaseRequestHandler requestHandler;
  private boolean commitAfterBatch;
  
  protected Counter activeRevsDiffRequests;
  protected Counter meanRevsDiffRequests;
  protected Counter activeBulkDocsRequests;
  protected Counter meanBulkDocsRequests;
  protected Counter totalTooManyConcurrentRequestsErrors;

  public SolrCAPIBehaviour(CouchbaseReplica couchbase, TypeSelector typeSelector, Map<String, String> documentTypeParentFields, Map<String, String> documentTypeRoutingFields, boolean commitAfterBatch) {
    this.couchbase = couchbase;
    this.requestHandler = couchbase.getRequestHandler();
    this.typeSelector = typeSelector;
    this.documentTypeParentFields = documentTypeParentFields;
    this.documentTypeRoutingFields = documentTypeRoutingFields;
    this.commitAfterBatch = commitAfterBatch;
    
     activeRevsDiffRequests = new Counter();
     meanRevsDiffRequests = new Counter();
     activeBulkDocsRequests = new Counter();
     meanBulkDocsRequests = new Counter();
     totalTooManyConcurrentRequestsErrors = new Counter();
  }
  
  public Map<String, Object> welcome() {
    Map<String,Object> responseMap = new HashMap<String, Object>();
    responseMap.put("welcome", "solr-couchbase-plugin");
    return responseMap;
  }
  
  public String databaseExists(String database) {
    String bucketName = getBucketNameFromDatabase(database);
    if(couchbase.getBucket(bucketName) != null) {
        return null;
    }
    return "missing";
  }
  
  protected String getBucketNameFromDatabase(String database) {
    String[] pieces = database.split("/", 2);
    if(pieces.length < 2) {
        return database;
    } else {
        return pieces[0];
    }
  }
  
  public Map<String, Object> getDatabaseDetails(String database) {
      String doesNotExistReason = databaseExists(database);
      if(doesNotExistReason == null) {
          Map<String, Object> responseMap = new HashMap<String, Object>();
          responseMap.put("db_name", getDatabaseNameWithoutUUID(database));
          return responseMap;
      }
      return null;
  }
  
  protected String getDatabaseNameWithoutUUID(String database) {
    int semicolonIndex = database.indexOf(';');
    if(semicolonIndex >= 0) {
        return database.substring(0, semicolonIndex);
    }
    return database;
  }
  
  public boolean createDatabase(String database) {
      // FIXME add test
      return false;
  }
  
  public boolean deleteDatabase(String database) {
      // FIXME add test
      return false;
  }
  
  public boolean ensureFullCommit(String database) {
      if("default".equals(database)) {
          return true;
      }
      return false;
  }
  
  public Map<String, Object> revsDiff(String database,
          Map<String, Object> revsMap) {
    long start = System.currentTimeMillis();
    activeBulkDocsRequests.inc();
    
    Map<String, Object> responseMap = null;
    
    String bucketName = getBucketNameFromDatabase(database);
    if(couchbase.getBucket(bucketName) != null) {
      responseMap = new HashMap<String, Object>();
        for (Entry<String, Object> entry : revsMap.entrySet()) {
            RefCounted<SolrIndexSearcher> searcher = requestHandler.getCore().getSearcher();
            String id = entry.getKey();
            String revs = (String) entry.getValue();
            TermQuery tQuery = new TermQuery(new Term(id));
            PrefixQuery pQuery = new PrefixQuery(new Term(id + "-"));
            BooleanQuery query = new BooleanQuery();
            query.add(tQuery, Occur.SHOULD);
            query.add(pQuery, Occur.SHOULD);
            try {
              DocSet docs = searcher.get().getDocSet(query, (DocSet)null);
              DocIterator iterator = docs.iterator();
              if(docs.size()>0) {
                while(iterator.hasNext()) {
                  int docId = iterator.nextDoc();
                  if(!revs.equals(searcher.get().doc(docId).get(CommonConstants.REVISION_FIELD))) {
                    Map<String, Object> rev = new HashMap<String, Object>();
                    rev.put("missing", revs);
                    responseMap.put(id, rev);
                  }
                }
              } else {
            	  Map<String, Object> rev = new HashMap<String, Object>();
                rev.put("missing", revs);
                responseMap.put(id, rev);
              }
            } catch (IOException e) {
              LOG.error("Could not do revsDiff!", e);
            } finally {
            	searcher.decref();
            }
        }
    }
    long end = System.currentTimeMillis();
    meanRevsDiffRequests.inc(end - start);
    activeRevsDiffRequests.dec();
    return responseMap;
  }
  
  public List<Object> bulkDocs(String database, List<Map<String, Object>> docs) {

    long start = System.currentTimeMillis();
    activeBulkDocsRequests.inc();
    
    String bucketName = getBucketNameFromDatabase(database);
    SolrQueryRequest req = new SolrQueryRequestBase(requestHandler.getCore(), new SolrParams() {
      
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
    // keep a map of the id - rev for building the response
    List<Object> bulkDocsResult = new ArrayList<Object>();
    Map<String,String> revisions = new HashMap<String, String>();
    
    if(couchbase.getBucket(bucketName) != null) {

        for (Map<String, Object> doc : docs) {

          // these are the top-level elements that could be in the document sent by Couchbase
          Map<String, Object> meta = (Map<String, Object>)doc.get("meta");
          String metaJson = JSONUtil.toJSON(meta);
          Map<String, Object> jsonMap = (Map<String, Object>)doc.get("json");
          String base64 = (String)doc.get("base64");
          String jsonString = null;
          
          if(meta == null) {
            // if there is no meta-data section, there is nothing we can do
            LOG.warn("Document without meta in bulk_docs, ignoring....");
            continue;
          } else if("non-JSON mode".equals(meta.get("att_reason"))) {
              // optimization, this tells us the body isn't json
              jsonMap = new HashMap<String, Object>();
          } else if(jsonMap == null && base64 != null) {
              byte[] decodedData = Base64.decodeBase64(base64);
              try {
                jsonString = new String(decodedData, "UTF-8");
                // now try to parse the decoded data as json
                jsonMap = (Map<String, Object>) mapper.readValue(decodedData, Map.class);
              }
              catch(Exception e) {
                  LOG.error("Unable to parse decoded base64 data as JSON, indexing stub for id: {}", meta.get("id"));
                  LOG.error("Body was: {} Parse error was: {}", new String(decodedData), e);
                    jsonMap = new HashMap<String, Object>();
 
                }
            }
            
            // at this point we know we have the document meta-data
          // and the document contents to be indexed are in json

          String id = (String)meta.get("id");
          String rev = (String)meta.get("rev");
          
          SolrInputDocument solrDoc = new SolrInputDocument();
          solrDoc.addField(CommonConstants.ID_FIELD, id);
          solrDoc.addField(CommonConstants.REVISION_FIELD, rev);
          solrDoc.addField(CommonConstants.JSON_FIELD, jsonString);
          solrDoc.addField(CommonConstants.METADATA_FIELD, metaJson);
          
          Map<String, Object> toBeIndexed = new HashMap<String, Object>();
          toBeIndexed.put("meta", meta);
          toBeIndexed.put("doc", jsonMap);

          long ttl = 0;
          Integer expiration = (Integer)meta.get("expiration");
          if(expiration != null) {
              ttl = (expiration.longValue() * 1000) - System.currentTimeMillis();
          }
          if(ttl > 0) {
            solrDoc.addField(CommonConstants.TTL_FIELD, ttl);
          }
          
          boolean deleted = meta.containsKey("deleted") ? (Boolean)meta.get("deleted") : false;
          
          if(!deleted) {
            String parentField = null;
            String routingField = null;
            String type = typeSelector.getType(bucketName, id);
            if(documentTypeParentFields != null && documentTypeParentFields.containsKey(type)) {
                parentField = documentTypeParentFields.get(type);
            }
            if(documentTypeRoutingFields != null && documentTypeRoutingFields.containsKey(type)) {
                routingField = documentTypeRoutingFields.get(type);
            }
            
            if(parentField != null) {
                Object parent = jsonMapPath(toBeIndexed, parentField);
                if (parent != null && parent instanceof String ) {
                  solrDoc.addField(CommonConstants.PARENT_FIELD, parent);
                } else {
                    LOG.warn("Unabled to determine parent value from parent field {} for doc id {}", parentField, id);
                }
            }
            if(routingField != null) {
                Object routing = jsonMapPath(toBeIndexed, routingField);
                if (routing != null && routing instanceof String) {
                  solrDoc.addField(CommonConstants.ROUTING_FIELD, routing);
                } else {
                    LOG.warn("Unable to determine routing value from routing field {} for doc id {}", routingField, id);
                }
            }
            
            //extract and map json fields
            JsonRecordReader rr = JsonRecordReader.getInst(couchbase.getBucket(bucketName).getSplitpath(),
                new ArrayList<String>(couchbase.getBucket(bucketName).getFieldmapping().values()));
            if(jsonString == null) {
              jsonString="";
            }
            JSONParser parser = new JSONParser(jsonString);
            Handler handler = new CouchbaseRecordHandler(this, req, solrDoc, bulkDocsResult);
            try {
              rr.streamRecords(parser, handler);
            } catch (IOException e) {
              LOG.error("Cannot parse Couchbase record!", e);
            }
          } else { //document deleted
            boolean success = deleteDoc(id, req);
            if(success) {
              Map<String, Object> itemResponse = new HashMap<String, Object>();
              itemResponse.put("id", id);
              itemResponse.put("rev", rev);
              if(!rev.equals(revisions.get(id))) {
                revisions.put(id, rev);
                bulkDocsResult.add(itemResponse);
              }
            }
          }
        }
        if(commitAfterBatch && bulkDocsResult.size() > 0) {
          commit(req);
        }
    } else {
      LOG.debug("Bucket \"" + bucketName + "\" is not configured with this plugin.");
    }
    
    long end = System.currentTimeMillis();
    meanBulkDocsRequests.inc(end - start);
    activeBulkDocsRequests.dec();
    return bulkDocsResult;
  }
  
  public Map<String, Object> getDocument(String database, String docId) {
      if("default".equals(database)) {
          if("docid".equals(docId)) {
              Map<String, Object> document = new HashMap<String, Object>();
              document.put("_id", "docid");
              document.put("_rev", "1-abc");
              document.put("value", "test");
              return document;
          }
      }
      return null;
  }
  
  public Map<String, Object> getLocalDocument(String database, String docId) {
      if("default".equals(database)) {
          if("_local/docid".equals(docId)) {
              Map<String, Object> document = new HashMap<String, Object>();
              document.put("_id", "_local/docid");
              document.put("_rev", "1-abc");
              document.put("value", "test");
              return document;
          } else if("_local/441-0921e80de6603d60b1d553bb7c253def/beer-sample/beer-sample".equals(docId)) {
              Map<String, Object> historyItem = new HashMap<String, Object>();
              historyItem.put("session_id", "121f9c416336108dd0b891a054f9b878");
              historyItem.put("start_time", "Thu, 30 Aug 2012 18:22:02 GMT");
              historyItem.put("end_time", "Thu, 30 Aug 2012 18:22:02 GMT");
              historyItem.put("start_last_seq", 0);
              historyItem.put("end_last_seq", 10);
              historyItem.put("recorded_seq", 10);
              historyItem.put("docs_checked", 10);
              historyItem.put("docs_written", 10);
  
              List<Object> history = new ArrayList<Object>();
              history.add(historyItem);
  
              Map<String, Object> document = new HashMap<String, Object>();
              document.put("session_id", "121f9c416336108dd0b891a054f9b878");
              document.put("source_last_seq", 10);
              document.put("start_time", "Thu, 30 Aug 2012 18:22:02 GMT");
              document.put("end_time", "Thu, 30 Aug 2012 18:22:02 GMT");
              document.put("docs_checked", 10);
              document.put("docs_written", 10);
              document.put("history", history);
              return document;
          }
      }
      return null;
  }
  
  public String storeDocument(String database, String docId,
          Map<String, Object> document) {
      // FIXME add test
      return null;
  }
  
  public String storeLocalDocument(String database, String docId,
          Map<String, Object> document) {
      // FIXME add test
      return null;
  }
  
  public InputStream getAttachment(String database, String docId,
          String attachmentName) {
      // FIXME add test
      return null;
  }
  
  public String storeAttachment(String database, String docId,
          String attachmentName, String contentType, InputStream input) {
      // FIXME add test
      return null;
  }
  
  public InputStream getLocalAttachment(String databsae, String docId,
          String attachmentName) {
      // FIXME add test
      return null;
  }
  
  public String storeLocalAttachment(String database, String docId,
          String attachmentName, String contentType, InputStream input) {
      // FIXME add test
      return null;
  }
  
  @Override
  public Map<String, Object> getStats() {
    Map<String, Object> stats = new HashMap<String, Object>();

    Map<String, Object> bulkDocsStats = new HashMap<String, Object>();
    bulkDocsStats.put("activeCount", activeBulkDocsRequests.count());
    bulkDocsStats.put("totalCount", meanBulkDocsRequests.count());
    bulkDocsStats.put("totalTime", meanBulkDocsRequests.sum());
    bulkDocsStats.put("avgTime", meanBulkDocsRequests.mean());

    Map<String, Object> revsDiffStats = new HashMap<String, Object>();
    revsDiffStats.put("activeCount", activeRevsDiffRequests.count());
    revsDiffStats.put("totalCount", meanRevsDiffRequests.count());
    revsDiffStats.put("totalTime", meanRevsDiffRequests.sum());
    revsDiffStats.put("avgTime", meanRevsDiffRequests.mean());

    stats.put("_bulk_docs", bulkDocsStats);
    stats.put("_revs_diff", revsDiffStats);
//    stats.put("tooManyConcurrentRequestsErrors", totalTooManyConcurrentRequestsErrors.count());

    return stats;
  }
  
  public String getVBucketUUID(String pool, String bucket, int vbucket) {
      if("default".equals(bucket)) {
          return "00000000000000000000000000000000";
      }
      return null;
  }
  
  public String getBucketUUID(String pool, String bucket) {
      if("default".equals(bucket)) {
          return "00000000000000000000000000000000";
      }
      return null;
  }
  
  public Object jsonMapPath(Map<String, Object> json, String path) {
    int dotIndex = path.indexOf('.');
    if (dotIndex >= 0) {
        String pathThisLevel = path.substring(0,dotIndex);
        Object current = json.get(pathThisLevel);
        String pathRest = path.substring(dotIndex+1);
        if (pathRest.length() == 0) {
            return current;
        }
        else if(current instanceof Map && pathRest.length() > 0) {
            return jsonMapPath((Map<String, Object>)current, pathRest);
        }
    } else {
        // no dot
        Object current = json.get(path);
        return current;
    }
    return null;
  }
  
  public boolean addDoc(SolrInputDocument doc, SolrQueryRequest req) {
    boolean success = false;
    try {
      AddUpdateCommand command = new AddUpdateCommand(req);
      command.solrDoc = doc;
      SolrQueryResponse rsp = new SolrQueryResponse();
      UpdateRequestProcessor processor = requestHandler.getProcessorChain().createProcessor(req, rsp);
      processor.processAdd(command);
      success = true;
    } catch (Exception e) {
      LOG.warn("Error creating document : " + doc, e);
    }
    return success;
  }
  
  public boolean deleteDoc(Object id, SolrQueryRequest req) {
    boolean success = false;
    try {
      LOG.info("Deleting document:" + id);
      DeleteUpdateCommand delCmd = new DeleteUpdateCommand(req);
      delCmd.setId(id.toString());
      SolrQueryResponse rsp = new SolrQueryResponse();
      UpdateRequestProcessor processor = requestHandler.getProcessorChain().createProcessor(req, rsp);
      processor.processDelete(delCmd);
      success = true;
    } catch (IOException e) {
      LOG.error("Exception while deleting doc:" + id, e);
    }
    return success;
  }

  public void commit(SolrQueryRequest req) {
    try {
      CommitUpdateCommand commit = new CommitUpdateCommand(req,false);
      SolrQueryResponse rsp = new SolrQueryResponse();
      UpdateRequestProcessor processor = requestHandler.getProcessorChain().createProcessor(req, rsp);
      processor.processCommit(commit);
    } catch (Exception e) {
      LOG.error("Exception while solr commit.", e);
    }
  }
}
