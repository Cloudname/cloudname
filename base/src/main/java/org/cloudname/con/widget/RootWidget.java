package org.cloudname.con.widget;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.cloudname.con.HttpConsole;
import org.cloudname.mon.Counter;


/**
 * 
 * Special root Widget for the HttpConsole
 * 
 * @author paulrene
 * @author borud
 *
 */
public class RootWidget implements HttpWidget {
    private final static Counter accessCounter = Counter.getCounter("sys.widget.root.get.count");

    private HttpConsole console;

    public void init(HttpConsole console) {
        this.console = console;
    }

    public ContentType getContentType() {
        return ContentType.TEXT_HTML;
    }
   
    public void requestReceived(String requestUri, Map<String, List<String>> requestParameters, List<Entry<String, String>> requestHeaders, String content, StringBuffer outputBuffer) {
        accessCounter.inc();

        outputBuffer.append("<!DOCTYPE html><html><head><title>RootWidget</title></head><body>");
        outputBuffer.append("<h2> Registered Paths</h2>");
        outputBuffer.append("<ul>");
        for (String path : console.getWidgetPaths()) {
            String cleanPath = path;
            if (cleanPath.endsWith("/*")) {
                cleanPath = path.substring(0, path.length() - 2);
            }

            // Don't bother listing the root path
            if ("/".equals(cleanPath)) {
                continue;
            }
            outputBuffer.append("<li><a href=\"").append(cleanPath).append("\">").append(cleanPath).append("</a>");
        }
        outputBuffer.append("</ul>");
        outputBuffer.append("</body></html>");
    }

    public void destroy() {
    }

}
