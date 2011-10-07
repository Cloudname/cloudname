package org.cloudname.con.netty;

import org.cloudname.con.HttpConsole;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

/**
 * 
 * Pileline factory
 * 
 * Creates ChannelPipeline for our HttpConsole
 * 
 * 
 * @author paulrene
 *
 */
public class HttpConsolePipelineFactory implements ChannelPipelineFactory {

    private HttpConsole console;

    public HttpConsolePipelineFactory(HttpConsole console) {
        this.console = console;
    }
    
    /* (non-Javadoc)
     * @see org.jboss.netty.channel.ChannelPipelineFactory#getPipeline()
     */
    public ChannelPipeline getPipeline() throws Exception {
        // Create a default pipeline implementation.
        ChannelPipeline pipeline = Channels.pipeline();

        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("encoder", new HttpResponseEncoder());
        // Remove the following line if you don't want automatic content compression.
        pipeline.addLast("deflater", new HttpContentCompressor());
        pipeline.addLast("handler", new HttpConsoleRequestHandler(console));
        
        return pipeline;
    }

}
