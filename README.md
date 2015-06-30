solr-couchbase-plugin
=====================

This plugin allows to import CouchBase data to Solr. It uses the Cross-datacenter Replication (XDCR) feature of Couchbase Server 2.0 to transfer data continuously.


# Installation
* Get Solr distribution
* Extract Solr war from `<extracted-solr>/server/webapps/solr.war` 
* Copy this plugin and its dependencies from `solr-war-libs/` directory to the extracted solr war directory. Dependencies should be located in the war file under:
```
<extracted-solr-war>/WEB-INF/lib/
```
* Assemble war from the `<extracted-solr-war>` and put it back to the `<extracted-solr>/server/webapps` directory overwriting original file.
* Start Solr


## solrconfig.xml

It is required to configure Couchbase plugin in `solrconfig.xml` file under */couchbase* RequestHandler. Whole RequestHandler configuration should look as following:

```
<requestHandler name="/couchbase" class="org.apache.solr.couchbase.CouchbaseRequestHandler">
    <lst name="params">
      <str name="username">Administrator</str>
      <str name="password">password</str>
      <int name="client_port">9876</int>
      <int name="numVBuckets">64</int>
      <bool name="commitAfterBatch">true</bool>
      <lst name="couchbaseServers">
        <str name="server1">127.0.0.1:8091</str>
        <str name="server2">127.0.0.1:9898</str>
      </lst>
      <str name="clusterName">solr</str>
    </lst>
    <lst name="bucket">
      <str name="name">beer-sample</str>
      <str name="splitpath">/</str>
      <lst name="fieldmappings">
        <str name="address">address_ss:/address</str>
        <str name="all">/*</str>
      </lst>
    </lst>
    <lst name="bucket">
      <str name="name">gamesim-sample</str>
      <str name="splitpath">/</str>
      <lst name="fieldmappings">
        <str name="all">/*</str>
      </lst>
    </lst>
    <lst name="bucket">
      <str name="name">test</str>
      <str name="splitpath">/exams</str>
      <lst name="fieldmappings">
        <str name="first">first_s:/first</str>
        <str name="last">last_s:/last</str>
        <str name="grade">grade_i:/grade</str>
        <str name="subject">subject_s:/exams/subject</str>
        <str name="test">test_s:/exams/test</str>
        <str name="marks">marks_i:/exams/marks</str>
        <str name="other">/*</str>
      </lst>
    </lst>
  </requestHandler>
```

* params - a list of params required to configure this plugin.
  - username - A valid Couchbase server username
  - password - A valid Couchbase server password
  - client_port - A port number on which this plugin will register itself as a Couchbase replica.
  - numVBuckets - A number of VBuckets used by this Couchbase replica. Couchbase Server on Mac OS X uses 64 vBuckets as opposed to the 1024 vBuckets used by other platforms. **Couchbase clusters with mixed platforms are not supported.** It is required that numVBuckets is identical on the Couchbase server and Solr plugin.
  - commitAfterBatch - A flag specifying whether this plugin should commit documents to Solr after every batch of documents or when all the documents are retrieved from Couchbase.
  - couchbaseServers - Required, a list of addresses of Couchbase Servers.
  - clusterName - Required. The cluster name that you want to create Couchbase XDCR remote cluster with.
  
* bucket - a list with bucket parameters required to perform a synchronisation with Couchbase. Multiple lists of this type are allowed. Couchbase plugin will automatically configure replication from the buckets specified in this list.
  - name - Bucket name - must be unique
  - splitpath - a list with paths to the fields in JSON Object on which the original Couchbase JSON document will be split up to extract embedded documents. This is a single String where paths are saparated with "|".
  - fieldmappings - a list with field names mapping for Couchbase documents, before indexing them into Solr. List element's name must be unique. At least one field mapping must be provided. Value should be `solr_field_name:couchbase_field_path`. The ‘json-path’ is a required part. 'target-field-name' is the name of the field in the input Solr document.  It is optional and it is automatically derived from the input json.
  - More informations about splitpath and fieldmappings usage can be found here: https://lucidworks.com/blog/indexing-custom-json-data/ 
  - Wildcards - Instead of specifying all the field names in *fieldmappings* explicitly , it is possible to specify a wildcard '\*' or a wildwildcard '\*\*' to map fields automatically. The constraint is that wild cards can be only used in the end of the json-path. The split path cannot use wildcards. The following are example wildcard path mappings:

Example Wildcards:
```
f=/docs/* : maps all the fields under docs and in the name as given in json
f=/docs/** : maps all the fields under docs and its children in the name as given in json
f=searchField:/docs/* : maps all fields under /docs to a single field called ‘searchField’
f=searchField:/docs/** : maps all fields under /docs and its children to searchField 
```


Example Bucket Configuration:
```
<lst name="bucket">
  <str name="name">default</str>
  <str name="splitpath">/</str>
  <lst name="fieldmappings">
    <str name="name">name:/name</str>
    <str name="city">city_s:/city</str>
    <str name="code">code_s:/code</str>
    <str name="country">country_s:/country</str>
    <str name="phone">phone_s:/phone</str>
    <str name="website">url:/website</str>
    <str name="type">type_s:/type</str>
    <str name="updated">last_modified:/updated</str>
    <str name="description">description:/description</str>
    <str name="address">address_s:/address</str>
    <str name="geo">geo_s:/geo</str>
  </lst>
</lst>
```    


Example JSON:
```
{
"person" : { 
            "name" : "John",
            "age" : 22
           },
"value" : 100
}
```

Example field mappings for above JSON:
```
<lst name="test-fieldmappings">
  <str name="person">person:/person</str>
  <str name="name">name:/person/name</str>
  <str name="age">age_i:/person/age</str>
  <str name="value">value_i:/value</str>
</lst>
```


## Couchbase XDCR
Couchbase plugin automatically configures replication from the buckets configured in `solrconfig.xml` file.

# Multiple collections

To sunchronize documents from Couchbase to multiple Solr collections, each collection needs to have this plugin configured in it's solrconfig.xml file and also a replication in Couchbase created. Each collection plugin must be started manually. If this is done, there will be one Couchbase replica running for each Solr collection.

# Running Solr Couchbase Plugin

To run this plugin, simply perform all actions described in Configuration section of this instruction, and run Solr. To run single instance of Solr, execute `ant run-solr` command in the main directory. To run Solr in Cloud mode, execute `ant solr-cloud` command in the main directory. When solr is started execute GET request to URL

```
http://<solr_address>/solr/collection1/couchbase?action=start
```

This will start the plugin, which will register in Couchbase as its replica and the data synchronisation as well as indexing into Solr should start. It assumes that Couchbase server is running and Cross-Datacenter Replication is set as described in *Couchbase XDCR* section of this document.

To stop the plugin, exegute GET request to the URL
```
http://<solr_address>/solr/collection1/couchbase?action=stop
```

In a case, when all configured Solr instances with this plugin are restarted, Couchbase XDCR must be configured again. This is because every time a Couchbase replica is started, it acquires new pool UUID which is used in communications with Couchbase server.
