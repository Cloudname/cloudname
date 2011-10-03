package org.cloudname.con.servlet;

import org.cloudname.con.WebConsole;
import org.cloudname.mon.Counter;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.TreeSet;
import java.util.Properties;
import java.io.PrintWriter;

/**
 * Special root servlet for the web console.
 *
 * @author borud
 */
public class RootServlet extends HttpServlet {
    private final static Counter accessCounter = Counter.getCounter("sys.servlet.root.get.count");

    private WebConsole con;

    public RootServlet(WebConsole con) {
        this.con = con;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException
    {
        accessCounter.inc();
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);

        PrintWriter w = response.getWriter();

        w.println("<h2> Registered Paths</h2>");
        w.println("<ul>");
        for (String path : con.getServletPaths()) {
            String cleanPath = path;
            if (cleanPath.endsWith("*")) {
                cleanPath = path.substring(0, path.length() - 1);
            }

            // Don't bother listing the root path
            if ("/".equals(cleanPath)) {
                continue;
            }

            w.println(" <li> <a href=\"" + cleanPath + "\">" + cleanPath + "</a>");
        }
        w.println("</ul>");
        w.flush();
    }
}