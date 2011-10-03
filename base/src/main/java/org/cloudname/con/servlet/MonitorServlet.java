package org.cloudname.con.servlet;

import org.cloudname.mon.MonitorManager;
import org.cloudname.mon.Counter;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.TreeSet;
import java.io.PrintWriter;

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
        w.flush();
    }
}