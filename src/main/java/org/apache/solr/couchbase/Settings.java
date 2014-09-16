package org.apache.solr.couchbase;

import java.util.HashMap;
import java.util.Map;

public class Settings extends HashMap<String,String>{

  /**
   * 
   */
  private static final long serialVersionUID = 1657776653494453826L;
  
  public String get(String key, String defaultValue) {
    String value = this.get(key);
    if(value == null) {
      value = defaultValue;
    }
    return value;
  }
  
  public Map<String,String> getByPrefix(String prefix) {
    Map<String,String> results = new HashMap<String, String>();
    
    for(Map.Entry<String, String> entry : this.entrySet()) {
      if(entry.getKey().startsWith(prefix)) {
        results.put(entry.getKey(), entry.getValue());
      }
    }
    
    return results;
  }
}
