package org.apache.solr.couchbase;

public class CommonConstants {

  /** Solr plugin params */
  public static final String HANDLER_PARAMS = "params";
  public static final String BUCKET_MARK = "bucket";
  		/** couchbase XDCR settings */
  public static final String COUCHBASE_SERVER_FIELD = "couchbaseServer";
  public static final String COUCHBASE_CLUSTER_NAME_FIELD = "clusterName";
  public static final String COUCHBASE_HOST_IP_FIELD = "ipAddress";
  public static final String COUCHBASE_USERNAME_FIELD = "couchbaseUsername";
  public static final String COUCHBASE_PASSWORD_FIELD = "couchbasePassword";
  public static final String COUCHBASE_FROM_BUCKET_INFO_FIELD = "bucketInfo";
  public static final String COUCHBASE_FROM_BUCKET_NAME_FIELD = "fromBucketName";
  public static final String COUCHBASE_TO_BUCKET_NAME_FIELD = "toBucketName";
  
  public static final String FIELD_MAPPING_FIELD = "fieldmappings";
  public static final String SPLITPATH_FIELD = "splitpath";
  public static final String USERNAME_FIELD = "username";
  public static final String PASSWORD_FIELD = "password";
  public static final String NUM_VBUCKETS_FIELD = "numVBuckets";
  public static final String HOST_IP = "hostIP";
  public static final String PORT_FIELD = "port";
  public static final String NAME_FIELD = "name";
  public static final String COMMIT_AFTER_BATCH_FIELD = "commitAfterBatch";
  /** Select the update processor chain to use.  A RequestHandler may or may not respect this parameter */
  public static final String UPDATE_CHAIN = "update.chain";
  
  /** Couchbase document fields */
  public static final String ID_FIELD = "id";
  public static final String REVISION_FIELD = "revision_s";
  public static final String JSON_FIELD = "content";
  public static final String METADATA_FIELD = "metadata_s";
  public static final String TTL_FIELD = "ttl_l";
  public static final String DELETED_FIELD = "deleted_b";
  public static final String PARENT_FIELD = "parent_s";
  public static final String ROUTING_FIELD = "routing_s";
}
