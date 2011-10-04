package org.cloudname.con.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.cloudname.mon.AverageLongData;
import org.cloudname.mon.Counter;
import org.cloudname.mon.HistogramCounter;
import org.cloudname.mon.MonitorManager;

public class MonitorServlet extends HttpServlet {
    private final static Counter accessCounter = Counter.getCounter("sys.servlet.monitor.get.count");
    private final static long startTime = System.currentTimeMillis();

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException
    {
        accessCounter.inc();

        response.setContentType("text/plain");
        response.setStatus(HttpServletResponse.SC_OK);
        PrintWriter w = response.getWriter();

        // Print start time and uptime
        w.println("sys.time.start = " + startTime);
        w.println("sys.time.uptime = " + (System.currentTimeMillis() - startTime));

        MonitorManager manager = MonitorManager.getInstance();
        for (String name : new TreeSet<String>(manager.getCounterNames())) {
            long count = manager.getCounter(name).getCount();
            w.println("count."+name + " = " + count);
        }
        for (String name : new TreeSet<String>(manager.getVariableNames())) {
            long value = manager.getVariable(name).getValue();
            w.println("var."+name + " = " + value);
        }
        for (String name : new TreeSet<String>(manager.getAverageLongNames())) {
            AverageLongData value = manager.getAverageLong(name).getRecords();
            w.println("AGGREGATED."+name + " = " + value.getAggregated());
            w.println("COUNTER."+name + " = " + value.getCount());
        }
        for (String name : new TreeSet<String>(manager.getHistogramCounterNames())) {
            HistogramCounter v = manager.getHistogramCounter(name);
            List<Entry<Long, Long>> records = v.getEntries();
            long previous = -1;
            for (Entry<Long, Long> entry : records) {
                String text = "";
                if (previous == -1) {
                    text = entry.getKey()+"+";
                } else {
                    text = entry.getKey()+"-"+previous;
                }
                previous = entry.getKey()-1;
                w.println("histogram."+name+"."+text+" = "+entry.getValue());
            }
        }
        w.flush();
    }
}