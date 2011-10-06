package org.cloudname.con.widget;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeSet;

import org.cloudname.con.HttpConsole;
import org.cloudname.mon.Counter;


/**
 * 
 * Simple Widget to list system properties
 * 
 * @author paulrene
 * @author borud
 *
 */
public class SystemPropertiesWidget implements HttpWidget {
    private final static Counter accessCounter = Counter.getCounter("sys.widget.properties.get.count");
    
    public void init(HttpConsole console) {
    }

    public ContentType getContentType() {
        return ContentType.TEXT_PLAIN;
    }
   
    public void requestReceived(String requestUri, Map<String, List<String>> requestParameters, List<Entry<String, String>> requestHeaders, String content, StringBuffer outputBuffer) {
        accessCounter.inc();

        Properties props = System.getProperties();
        for (String name : new TreeSet<String>(props.stringPropertyNames())) {
            String value = props.getProperty(name);
            outputBuffer.append(name).append(" = ").append(value).append(newline);
        }
    }

    public void destroy() {
    }

}
