package org.cloudname.con.servlet;

import org.cloudname.mon.Counter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.TreeSet;
import java.util.Properties;
import java.io.PrintWriter;

/**
 * Simple servlet to list system properties.
 *
 * @author borud
 */
public class SystemPropertiesServlet extends HttpServlet {
    private final static Counter accessCounter = Counter.getCounter("sys.servlet.properties.get.count");

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException
    {
        accessCounter.inc();
        response.setContentType("text/plain");
        response.setStatus(HttpServletResponse.SC_OK);

        PrintWriter w = response.getWriter();
        Properties props = System.getProperties();

        for (String name : new TreeSet<String>(props.stringPropertyNames())) {
            String value = props.getProperty(name);
            w.println(name + " = " + value);
        }
        w.flush();
    }
}