package org.apache.solr.couchbase;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;

import org.codehaus.jettison.json.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.couchbase.common.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class CouchbaseUtils {
	private static final Logger LOG = LoggerFactory.getLogger(CouchbaseUtils.class);
	
	private static String couchbaseIp = null;
	private static String couchbaseUsername = null;
	private static String couchbasePassword = null;
	private static String clusterName = null;
	private static List<String> fromBuckets = new ArrayList<>();
	private static List<String> toBuckets = new ArrayList<>();
	
	/**
	   * Method creating Couchbase XDCR remote cluster and XDCR replication(s). 
	   * @param NamedList couchbaseServer representing the configuration for Couchbase XDCR setting. 
	   * @param cbClientHost representing the host IP for Couchbase client.
	   * @param cbClientPort representing the port for Couchbase client.
	   * @param cbClientUsername representing the username for Couchbase client.
	   * @param cbClientPassword representing the password for Couchbase client.
	   * See README.md for sample format.
	   */
	public static void createCouchbaseXDCRReplications(NamedList couchbaseServer, 
														String cbClientHost, String cbClientPort,
														String cbClientUsername, String cbClientPassword) {
		boolean initSuc = init(couchbaseServer);
		String uuid = null;
		if(initSuc) {
			uuid = createRemoteClusters(cbClientUsername, cbClientPassword, cbClientHost, cbClientPort);
		}
		if(uuid != null) {
			createReplication(uuid);
		}
	}
	
	  /**
	   * Method parsing NamedList which passed from CouchbaseRequestHandler. 
	   * @param NamedList couchbaseServer representing the configuration for Couchbase XDCR setting. 
	   * See README.md for sample format.
	   */
	private static boolean init(NamedList couchbaseServer) {
		// parse basic information for creating Couchbase remote cluster
		couchbaseIp = (String)couchbaseServer.get(CommonConstants.COUCHBASE_HOST_IP_FIELD);
		// TODO: May want to consider to encrypt username and password in solrconfig.xml 
		couchbaseUsername = (String)couchbaseServer.get(CommonConstants.COUCHBASE_USERNAME_FIELD);
		couchbasePassword = (String)couchbaseServer.get(CommonConstants.COUCHBASE_PASSWORD_FIELD);
		clusterName = (String)couchbaseServer.get(CommonConstants.COUCHBASE_CLUSTER_NAME_FIELD);
		
		if(couchbaseIp == null || couchbaseUsername == null || couchbasePassword == null || clusterName == null) {
			LOG.warn("couchbaseServer configuration is missing ipAddress/couchbaseUsername/couchbasePassword/clusterName attribute(s)");
			return false;
		}
		
		// parse buckets information
		List<NamedList<Object>> bucketsList = couchbaseServer.getAll(CommonConstants.COUCHBASE_FROM_BUCKET_INFO_FIELD);
		for(NamedList bucketInfo : bucketsList) {
			String fromBucket = (String)bucketInfo.get(CommonConstants.COUCHBASE_FROM_BUCKET_NAME_FIELD);
			String toBucket = (String)bucketInfo.get(CommonConstants.COUCHBASE_TO_BUCKET_NAME_FIELD);
			
			if(fromBucket != null && toBucket != null) {
				fromBuckets.add(fromBucket);
				toBuckets.add(toBucket);
			}
			else {
				LOG.warn("couchbaseServer bucketInfo configuration is missing fromBucketName/toBucketName attribute(s)");
			}
		}
		return true;	
	}
	
	// Creating destination cluster reference
	private static String createRemoteClusters(String cbClientUsername, String cbClientPassword, String host, String port) {
		String uuid = getRemoteClusterRef(cbClientUsername, cbClientPassword, host, port);
		
		if(uuid != null) {	
			ClientConfig config = new DefaultClientConfig();
		    Client client = Client.create(config);
		    client.addFilter(new HTTPBasicAuthFilter(couchbaseUsername, couchbasePassword));
		    	
		    String url = "http://" + couchbaseIp + ":" + "8091";
		    WebResource couchbaseService = client.resource(UriBuilder.fromUri(url).build());
		   
		    MultivaluedMap formData = new MultivaluedMapImpl();
		    formData.add("uuid", uuid);
		    formData.add("name", clusterName);
		    formData.add("hostname", host + ":" + port);
		    formData.add("username", cbClientUsername);
		    formData.add("password", cbClientPassword);	

		    ClientResponse response = couchbaseService.path("pools").path("default").path("remoteClusters").type(MediaType.APPLICATION_FORM_URLENCODED_TYPE).post(ClientResponse.class, formData);     	
		    if(response.getStatus() == 200) {
		    	LOG.debug("CouchbaseUtils.createRemoteClusters : " + "Remote cluter = " + clusterName + " created.");
		    	return uuid;
		    }
		    else {
		    	LOG.warn("CouchbaseUtils.createRemoteClusters : " + "Remote cluter = " + clusterName + " creation failed with msg = " + response.getEntity(String.class));
		    }
		}
		return null;
	}
	
    // Getting destination cluster reference. Notice that the destination/remote cluster here is this plugin itself
    private static String getRemoteClusterRef(String cbClientUsername, String cbClientPassword, String host, String port) {	
    	ClientConfig remoteConfig = new DefaultClientConfig();
    	Client remoteClient = Client.create(remoteConfig);
    	remoteClient.addFilter(new HTTPBasicAuthFilter(cbClientUsername, cbClientPassword));
    	
    	String url = "http://" + host + ":" + port;
    	WebResource remoteServerService = remoteClient.resource(UriBuilder.fromUri(url).build());
    
		String  uuid = null;
			
		try {
			JSONObject obj = new JSONObject(remoteServerService.path("pools").path("default").path("remoteCluster").accept(MediaType.APPLICATION_JSON).get(String.class));
			obj = obj.getJSONObject("buckets");
        	String uri = obj.getString("uri");
        	uuid = uri.substring(uri.indexOf("uuid=")+"uuid=".length());
	    }
	    catch(Exception e) {
	        LOG.warn("CouchbaseUtils.getRemoteClusterRef : " + e.getMessage());
	    }	
    	return uuid;
    }
    
    // Create XDCR replications
    public static void createReplication(String uuid) {
    	ClientConfig config = new DefaultClientConfig();
	    Client client = Client.create(config);
	    client.addFilter(new HTTPBasicAuthFilter(couchbaseUsername, couchbasePassword));
	    	
	    String url = "http://" + couchbaseIp + ":" + "8091";
	    WebResource couchbaseService = client.resource(UriBuilder.fromUri(url).build());
    	
	    for(int i=0; i<fromBuckets.size(); i++) {
	    	MultivaluedMap formData = new MultivaluedMapImpl();
    		formData.add("uuid", uuid);
    		formData.add("toCluster", clusterName);
    		formData.add("fromBucket", fromBuckets.get(i));
    		formData.add("toBucket", toBuckets.get(i));
    		formData.add("replicationType", "continuous");	
    		formData.add("type", "capi");	

    		ClientResponse response = couchbaseService.path("controller").path("createReplication").type(MediaType.APPLICATION_FORM_URLENCODED_TYPE).post(ClientResponse.class, formData);
    		
    		if(response.getStatus() == 200) {
		    	LOG.debug("CouchbaseUtils.createReplication : " + "Created Couchbase replication from Couchbase bukcet = " + fromBuckets.get(i) + " to Couchbase client bucket = " + toBuckets.get(i));
		    }
		    else {
		    	LOG.warn("CouchbaseUtils.createReplication : " + "Failed to create Couchbase replication from Couchbase bukcet = " + fromBuckets.get(i) + " to Couchbase client bucket = " + toBuckets.get(i));
		    }
	    }
    }
    
    public static int getNumVBuckets() {
    	// TODO get num_vbuckets from Couchbase Server
    	return 64;
    }
}
