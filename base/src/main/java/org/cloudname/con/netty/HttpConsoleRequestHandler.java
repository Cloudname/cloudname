package org.cloudname.con.netty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.cloudname.con.HttpConsole;
import org.cloudname.con.widget.HttpWidget;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpChunkTrailer;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.util.CharsetUtil;

/**
 * HTTP Request dispatcher
 * 
 * Processes a HTTP request and dispatches the request to Widgets
 * 
 * @author paulrene
 * 
 */
public class HttpConsoleRequestHandler extends SimpleChannelUpstreamHandler {

    private HttpConsole console;
    private HttpRequest request;
    private boolean processingChunks;
    private String uri;
    private List<Entry<String, String>> headers;
    private List<Entry<String, String>> trailingHeaders;
    private Map<String, List<String>> parameters;
    private StringBuffer contentBuffer;
    private StringBuffer outputBuffer;
    private String requestPath;

    public HttpConsoleRequestHandler(HttpConsole console) {
        this.console = console;
        outputBuffer = new StringBuffer();
        contentBuffer = new StringBuffer();
    }

    /* (non-Javadoc)
     * @see org.jboss.netty.channel.SimpleChannelUpstreamHandler#messageReceived(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.MessageEvent)
     */
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if (!processingChunks) {
            contentBuffer.setLength(0);
            outputBuffer.setLength(0);
            
            request = (HttpRequest) e.getMessage();

            // Check if the message contains the "Expect: 100-continue" header
            if (HttpHeaders.is100ContinueExpected(request)) { // ..and act accordingly
                HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE);
                e.getChannel().write(response);
            }

            uri = request.getUri();

            QueryStringDecoder qsd = new QueryStringDecoder(uri);
            parameters = qsd.getParameters();
            
            requestPath = qsd.getPath();

            headers = request.getHeaders();

            if (!(processingChunks = request.isChunked())) {
                ChannelBuffer content = request.getContent();
                if (content.readable()) {
                    contentBuffer.append(content.toString(CharsetUtil.UTF_8));
                }
                createResponse(e);
            }
        } else { // If this is a chunked http request, this code will aggregate it into the contentBuffer
            HttpChunk chunk = (HttpChunk) e.getMessage();
            if (chunk.isLast()) {
                processingChunks = false;

                HttpChunkTrailer trailer = (HttpChunkTrailer) chunk;
                trailingHeaders = trailer.getHeaders();

                createResponse(e);
            } else {
                contentBuffer.append(chunk.getContent().toString(CharsetUtil.UTF_8));
            }
        }
    }

    /**
     * Will try to locate the Widgets that is configured to handle the given request URL.
     * 
     * It will choose the most complex Content Type and use that for the response, chaining the content of the Widgets together.
     * NOTE: This chaining has been disabled in the HttpConsole.addWidget method for the time being.
     * 
     * @param event
     */
    private void createResponse(MessageEvent event) {
        HttpResponseStatus status = HttpResponseStatus.OK;
        HttpWidget.ContentType chosenContentType = HttpWidget.ContentType.getDefault();

        ArrayList<HttpWidget> widgetList = console.getMatchingWidgets(requestPath);
        
        if (widgetList.isEmpty()) {
            status = HttpResponseStatus.NOT_FOUND;
        } else {
            for (HttpWidget widget : widgetList) {
                HttpWidget.ContentType widgetContentType = widget.getContentType();
                if(widgetContentType.getPriority()>chosenContentType.getPriority()) {
                    chosenContentType = widgetContentType;
                }
                try {
                    widget.requestReceived(uri, parameters, headers, contentBuffer.toString(), outputBuffer);
                } catch (RuntimeException ex) {
                    status = HttpResponseStatus.INTERNAL_SERVER_ERROR;
                }
            }
        }

        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
        response.setContent(ChannelBuffers.copiedBuffer(outputBuffer.toString(), CharsetUtil.UTF_8));
        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, chosenContentType.getDeclaration()+"; charset=UTF-8");
        
        boolean keepAlive = HttpHeaders.isKeepAlive(request);
        
        if(keepAlive) {
            // Set content length header only for keep alive connections
            response.setHeader(HttpHeaders.Names.CONTENT_LENGTH, response.getContent().readableBytes());
        }
        
        ChannelFuture future = event.getChannel().write(response);

        if(!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }

    }

}
