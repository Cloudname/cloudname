package org.cloudname.con.widget;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.cloudname.con.HttpConsole;
import org.cloudname.mon.Counter;
import org.cloudname.mon.MonitorManager;


/**
 * 
 * Widget that lists all counters of the global monitor
 * 
 * @author paulrene
 * @author borud
 *
 */
public class MonitorWidget implements HttpWidget {
    private final static Counter accessCounter = Counter.getCounter("sys.widget.monitor.get.count");
    
    private long startTime;

    public void init(HttpConsole console) {
        startTime = System.currentTimeMillis();
    }

    public ContentType getContentType() {
        return ContentType.TEXT_PLAIN;
    }
   
    public void requestReceived(String requestUri, Map<String, List<String>> requestParameters, List<Entry<String, String>> requestHeaders, String content, StringBuffer outputBuffer) {
        accessCounter.inc();

        // Print start time and uptime
        outputBuffer.append("sys.time.start = ").append(startTime).append(newline);
        outputBuffer.append("sys.time.uptime = ").append(System.currentTimeMillis() - startTime).append(newline);

        MonitorManager manager = MonitorManager.getInstance();
        for (String name : new TreeSet<String>(MonitorManager.getCounterNames())) {
            long count = manager.getCounter(name).getCount();
            outputBuffer.append("count.").append(name).append(" = ").append(count).append(newline);
        }
        for (String name : new TreeSet<String>(MonitorManager.getVariableNames())) {
            long value = manager.getVariable(name).getValue();
            outputBuffer.append("var.").append(name).append(" = ").append(value).append(newline);
        }
    }

    public void destroy() {
    }

}
