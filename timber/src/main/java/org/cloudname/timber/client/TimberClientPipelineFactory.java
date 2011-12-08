package org.cloudname.timber.client;

import org.cloudname.log.pb.Timber;

import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;


/**
 * Client pipeline factory.
 *
 * @author borud
 */
public class TimberClientPipelineFactory  implements ChannelPipelineFactory {
    private TimberClient client;
    private TimberClientHandler clientHandler;

    /**
     * Create a pipeline for the timber client.
     *
     * @param client
     */
    public TimberClientPipelineFactory(TimberClient client) {
        this.client = client;
        clientHandler = new TimberClientHandler(client);
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = Channels.pipeline();

        p.addLast("frameDecoder", new ProtobufVarint32FrameDecoder());
        p.addLast("protobufDecoder", new ProtobufDecoder(Timber.LogEvent.getDefaultInstance()));

        p.addLast("frameEncoder", new ProtobufVarint32LengthFieldPrepender());
        p.addLast("protobufEncoder", new ProtobufEncoder());
        p.addLast("handler", clientHandler);
        return p;
    }
}