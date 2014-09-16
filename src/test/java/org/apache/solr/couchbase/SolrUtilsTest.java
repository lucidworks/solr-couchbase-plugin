package org.apache.solr.couchbase;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;

public class SolrUtilsTest extends TestCase {

  @Test
  public void testMappingToDynamicFields() {

    Map<String,Object> map = new HashMap<String, Object>();
    map.put("last_name", "Smith");
    map.put("age", 30);
    map.put("value", 4356L);
    map.put("shoe", 8.5f);
    map.put("height", 5.6);
  }
}
