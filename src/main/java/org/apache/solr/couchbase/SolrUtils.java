package org.apache.solr.couchbase;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SolrUtils {
  
  /**
   * Set of suffixes being recognixed by the mapping methods 
   */
  private static final Set<String> suffixes = new HashSet<String>(Arrays.asList("_b", "_d", "_dt", "_f", "_i", "_l", "_txt", "_s"));

  public static Set<String> getSuffixes() {
    return suffixes;
  }
  /**
   * Method mapping field names to Solr dynamic fields based on the value type
   * @param jsonMap A Map<String,Object> representing JSON document
   * @param fieldmapping A map of name -> field_mapping entries. Field mapping stucture is <dynamic_field>:<JSON_path>.
   */
  public static Map<String, String> mapToSolrDynamicFields(Map<String,Object> jsonMap) {
    Map<String, String> solrMapping = new HashMap<String, String>();
    for(Map.Entry<String, Object> entry : jsonMap.entrySet()) {
      String key = entry.getKey();
      int keyLength = key.length();

      if(keyLength == 0) {
        continue;
      }
      if(keyLength < 3) { //key too short to have dynamic mapping, don't check it.
      } else if(keyLength == 3) {
        if(suffixes.contains(key.substring(key.length()-2, key.length()))) { //one-letter key
          continue;
        }
      } else if(keyLength > 3) {
        if(suffixes.contains(key.substring(key.length()-2, key.length()))
            || SolrUtils.getSuffixes().contains(key.substring(key.length()-3, key.length()))
            || SolrUtils.getSuffixes().contains(key.substring(key.length()-4, key.length()))) {//other length keys
          continue;
        }
      }
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
  
  /**
   * Returns corresponding Solr dynamic field suffix for the passed Object
   * @param o
   * @return corresponding Solr dynamic field suffix
   */
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
