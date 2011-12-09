package org.cloudname.timber.server;

import org.cloudname.log.pb.Timber;

import org.jboss.netty.channel.Channels;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;

/**
 * The ChannelPipelineFactory for Timber.
 *
 * @author borud
 */
public class TimberServerPipelineFactory implements ChannelPipelineFactory {
    private final Dispatcher dispatcher;

    public TimberServerPipelineFactory(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    /**
     * Create a pipeline for encoding and decoding frames with
     * protobuffer payloads.
     */
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = Channels.pipeline();
        p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
        p.addLast("protobufDecoder", new ProtobufDecoder(Timber.LogEvent.getDefaultInstance()));

        p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
        p.addLast("protobufEncoder", new ProtobufEncoder());

        p.addLast("handler", new TimberServerHandler(dispatcher));
        return p;
    }
}
