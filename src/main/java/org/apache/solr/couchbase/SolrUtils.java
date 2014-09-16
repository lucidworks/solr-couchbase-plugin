package org.apache.solr.couchbase;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SolrUtils {

  /**
   * Method mapping field names to Solr dynamic fields based on the value type
   * @param jsonMap A Map<String,Object> representing JSON document
   * @param fieldmapping A map of name -> field_mapping entries. Field mapping stucture is <dynamic_field>:<JSON_path>.
   */
  public static Map<String, String> mapToSolrDynamicFields(Map<String,Object> jsonMap) {
    Map<String, String> solrMapping = new HashMap<String, String>();
    for(Map.Entry<String, Object> entry : jsonMap.entrySet()) {
      Object value = entry.getValue();
      String suffix = getSuffixFromObject(value);
      if(suffix.equals("_map")) {
        solrMapping.putAll(mapToSolrDynamicFields((Map<String, Object>) value));
      } else {
        solrMapping.put(entry.getKey(), entry.getKey() + suffix);
      }
    }
    return solrMapping;
  }
  
  public static String getSuffixFromObject(Object o) {
    String suffix = "_txt";
    if(o instanceof String) {
      suffix = "_s";
    } else if(o instanceof Integer) {
      suffix = "_i";
    } else if(o instanceof Long) {
      suffix = "_l";
    } else if(o instanceof Boolean) {
      suffix = "_b";
    } else if(o instanceof Float) {
      suffix = "_f";
    } else if(o instanceof Double) {
      suffix = "_d";
    } else if(o instanceof Date) {
      suffix = "_dt";
    } else if(o instanceof List<?>) {
      if(o != null && ((List)o).size() > 0)
      suffix = getSuffixFromObject(((List) o).get(0));
    } else if(o instanceof Map<?, ?>) {
      suffix = "_map";
    }
    return suffix;
  }
}
