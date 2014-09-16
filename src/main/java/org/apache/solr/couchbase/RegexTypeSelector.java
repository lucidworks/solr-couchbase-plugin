package org.apache.solr.couchbase;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegexTypeSelector implements TypeSelector {

    protected Logger LOG = LoggerFactory.getLogger(getClass());
    private String defaultDocumentType;
    private Map<String,String> documentTypePatternStrings;
    private Map<String, Pattern> documentTypePatterns;

    @Override
    public void configure(Settings settings) {
        this.defaultDocumentType = settings.get("couchbase.defaultDocumentType", DefaultTypeSelector.DEFAULT_DOCUMENT_TYPE_DOCUMENT);
        this.documentTypePatterns = new HashMap<String,Pattern>();
        this.documentTypePatternStrings = settings.getByPrefix("couchbase.documentTypes.");
        for (String key : documentTypePatternStrings.keySet()) {
            String pattern = documentTypePatternStrings.get(key);
            LOG.info("See document type: {} with pattern: {} compiling...", key, pattern);
            documentTypePatterns.put(key, Pattern.compile(pattern));
        }
    }

    @Override
    public String getType(String index, String docId) {
        for(Map.Entry<String,Pattern> typePattern : this.documentTypePatterns.entrySet()) {
            if(typePattern.getValue().matcher(docId).matches()) {
                return typePattern.getKey();
            }
        }
        return defaultDocumentType;
    }

}
