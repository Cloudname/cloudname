package org.cloudname.con.widget;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.cloudname.con.HttpConsole;

/**
 * 
 * Provides an interface to be implemented by Widgets wishing to handle HTTP requests
 * 
 * @author paulrene
 *
 */
public interface HttpWidget {
    
    // Helper when appending lines to the outputBuffer
    public static String newline = System.getProperty("line.separator");
    
    /**
     * Will be invoked once per HTTP request
     * 
     * All output should be appended to the outputBuffer.
     * If the HTTP request included a body it could be found in the content parameter.
     * 
     * @param requestUri
     * @param requestParameters
     * @param requestHeaders
     * @param content
     * @param outputBuffer
     */
    public void requestReceived(String requestUri, Map<String, List<String>> requestParameters, List<Entry<String, String>> requestHeaders, String content, StringBuffer outputBuffer);

    /**
     * This is the content-type that the HTTP response will use
     * 
     * @return
     */
    public ContentType getContentType();
    
    
    public static enum ContentType {
        TEXT_PLAIN(0, "text/plain"),
        TEXT_JSON(1, "application/json"),
        TEXT_XML(2, "text/xml"),
        TEXT_HTML(3, "text/html");

        private int priority;
        private String declaration;
        
        ContentType(int priority, String declaration) {
            this.priority = priority;
            this.declaration = declaration;
        }
        
        public int getPriority() {
            return priority;
        }
        
        public String getDeclaration() {
            return declaration;
        }

        public static ContentType getDefault() {
            return TEXT_PLAIN;
        }
    }


    
    /**
     * Will be invoked once before the first requestReceived invocation
     */
    public void init(HttpConsole console);

    
    /**
     * Will be invoked during server shutdown.
     * - It might be invoked during an ongoing request
     * - requestReceived will not be invoked after destroy is invoked
     */
    public void destroy();
    
}
