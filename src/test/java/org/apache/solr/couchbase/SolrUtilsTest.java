package org.apache.solr.couchbase;
import java.util.Calendar;
import java.util.Date;
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
    map.put("weight", null);
    map.put("married", true);
    Date time = Calendar.getInstance().getTime();
    map.put("birth_date", time);
    Map<String,String> object = new HashMap<String, String>();
    object.put("foo", "bar");
    object.put("bar", "baz");
    map.put("object", object);
    map.put("correct_s", "correct");
    map.put("correct_dt", time);
    map.put("correct_txt", "correct long text");
    map.put("a", "text");
    map.put("", "text");
    
    Map<String,String> mapped = SolrUtils.mapToSolrDynamicFields(map);
    assertEquals("last_name_s", mapped.get("last_name"));
    assertEquals("age_i", mapped.get("age"));
    assertEquals("value_l", mapped.get("value"));
    assertEquals("shoe_f", mapped.get("shoe"));
    assertEquals("height_d", mapped.get("height"));
    assertEquals("weight_txt", mapped.get("weight"));
    assertEquals("married_b", mapped.get("married"));
    assertEquals("birth_date_dt", mapped.get("birth_date"));
    assertEquals("foo_s", mapped.get("foo"));
    assertEquals("bar_s", mapped.get("bar"));
    assertEquals(null, mapped.get("correct_s"));
    assertEquals(null, mapped.get("correct_dt"));
    assertEquals(null, mapped.get("correct_txt"));
    assertEquals("a_s", mapped.get("a"));
    assertEquals(null, mapped.get(""));
  }
}
