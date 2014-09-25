package org.apache.solr.couchbase;

import java.util.UUID;

public class Utils {

  public static String randomID() {
    return UUID.randomUUID().toString().replaceAll("-", "");
  }
}
