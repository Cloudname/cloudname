package org.cloudname.timber.client;

import org.cloudname.log.pb.Timber;

import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import org.jboss.netty.bootstrap.ClientBootstrap;

/**
 * Client pipeline factory.
 *
 * @author borud
 */
public class TimberClientPipelineFactory  implements ChannelPipelineFactory {
    private TimberClient client;
    private ClientBootstrap bootstrap;
    private final TimberClientHandler handler;

    // Use common ReconnectDelayManager across all TimberClientHandler instances.
    private final ReconnectDelayManager reconnectDelayManager = new ReconnectDelayManager();

    /**
     * Create a pipeline for the timber client.
     *
     * @param client
     */
    public TimberClientPipelineFactory(TimberClient client, ClientBootstrap bootstrap) {
        this.client = client;
        this.bootstrap = bootstrap;
        handler = new TimberClientHandler(client, bootstrap, reconnectDelayManager);
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = Channels.pipeline();

        p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
        p.addLast("protobufDecoder", new ProtobufDecoder(Timber.AckEvent.getDefaultInstance()));

        p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
        p.addLast("protobufEncoder", new ProtobufEncoder());
        p.addLast("handler", handler);
        return p;
    }
}
