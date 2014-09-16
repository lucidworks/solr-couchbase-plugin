solr-couchbase-plugin
=====================

This plugin allows to import CouchBase data to Solr. It uses the Cross-datacenter Replication (XDCR) feature of Couchbase Server 2.0 to transfer data continuously.

# Plugin configuration


## Dependencies

* Copy the dependencies provided with this plugin to the directory:
```
<solr_home>/solr/lib-couchbase/
```
* Replace the *commons-io* dependency in solr.war file to the 2.4 version. It is located under:
```
<solr-war>/WEB-INF/lib/
```


## solr.xml

An additional line to solr.xml should be added, to inform Solr about new dependencies which should be included in the Solr's classpath. Add the following line to solr.xml file:

```
<str name="sharedLib">${sharedLib:lib-couchbase}</str>
```

The whole solr.xml file should look as follows:

```
<solr>
  <str name="sharedLib">${sharedLib:lib-couchbase}</str>

  <solrcloud>
    <str name="host">${host:}</str>
    <int name="hostPort">${jetty.port:8983}</int>
    <str name="hostContext">${hostContext:solr}</str>
    <int name="zkClientTimeout">${zkClientTimeout:30000}</int>
    <bool name="genericCoreNodeNames">${genericCoreNodeNames:true}</bool>
  </solrcloud>

  <shardHandlerFactory name="shardHandlerFactory"
    class="HttpShardHandlerFactory">
    <int name="socketTimeout">${socketTimeout:0}</int>
    <int name="connTimeout">${connTimeout:0}</int>
  </shardHandlerFactory>

</solr>
```


## solrconfig.xml

It is required to configure Couchbase buckets to index data from in the solrconfig.xml file under */couchbase* RequestHandler. Whole RequestHandler configuration should look as following:

```
<requestHandler name="/couchbase" class="org.apache.solr.couchbase.CouchbaseRequestHandler">
  <lst name="params">
    <str name="username">admin</str>
    <str name="password">admin123</str>
    <int name="port">9876</int>
    <bool name="commitAfterBatch">false</bool>
    <bool name="optimize">false</bool>
  </lst>
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
</requestHandler>
```

* params - a list of params required to configure this plugin.
  - username - A valid Couchbase server username
  - password - A valid Couchbase server password
  - port - A port number on which this plugin will register itself as a Couchbase replica.
  - commitAfterBatch - A flag specifying whether this plugin should commit documents to Solr after every batch of documents or when all the documents are retrieved from Couchbase.
  - optimize - Optimize parameter for Solr's commit request.
  
* bucket - a list with bucket parameters required to perform a synchronisation with Couchbase. Multiple lists of this type are allowed.
  - name - Bucket name - must be unique
  - splitpath - a list with paths to the fields in JSON Object on which the original Couchbase JSON document will be split up to extract embedded documents. This is a single String where paths are saparated with "|".
  - fieldmappings - a list with field names mapping for Couchbase documents, before indexing them into Solr. List element's name must be unique. Value should be `solr_field_name:couchbase_field_path`, where the `couchbase_field_path` is a path to the field in this JSON. Example:
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
Example JSON
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

In Couchbase admin panel, under XDCR tab following settings should be configured:

* Remote Cluster - This Solr plugin should be configured as Couchbase's Remote Cluster. Click on 'Create Cluster Reference' button. Fill in cluster data. 'IP/hostname" should be <ip_address>:port and the port number is the port on which this request handler will register itself as a Couchbase Replica. It is specified in CouchbaseRequestHandler's *params* list as 'port'.
 
* Replication - Click on 'Create Replication' button. Select a Couchbase Cluster and a Bucket to replicate from. Select configured Solr's Plugin Remote Cluster and add a Bucket name. This Bucket name must match the name specified in *databases*, *fieldmappings* and *splitpaths* in the solrconfig.xml file. Under *Advaced settings* field 'XDCR Prootcol' should be set to 'Version 1' value.


# Running Solr Couchbase Plugin

To run this plugin, simply perform all actions described in Configuration section of this instruction, and run Solr. To run single instance of Solr, execute `ant run-solr` command in the main directory. To run Solr in Cloud mode, execute `ant solr-cloud` command in the main directory. When solr is started execute GET request to URL

```
http://<solr_address>/solr/collection1/couchbase?action=start
```

This will start the plugin, which will register in Couchbase as its replica and the data synchronisation as well as indexing into Solr should start. It assumes that Couchbase server is running and Cross-Datacenter Replication is set as desribed in *Couchbase XDCR* section.c
