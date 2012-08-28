package org.cloudname.timber.server;

import org.jboss.netty.channel.AbstractChannel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.SucceededChannelFuture;
import org.jboss.netty.channel.ChannelSink;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineException;
import org.jboss.netty.channel.socket.SocketChannelConfig;

import java.net.InetSocketAddress;

import java.util.LinkedList;
import java.util.List;

/**
 * A very quick and dirty mock Channel implementation that is used for
 * unit tests.  It really does nothing meaningful except intercept
 * write.
 *
 * @author borud
 */
public class MockChannel extends AbstractChannel {
    private LinkedList<Object> writtenObjects = new LinkedList<Object>();

    /**
     * Construct a completely useless channel.
     */
    public MockChannel() throws Exception {
        super(
            null,
            null,
            new TimberServerPipelineFactory(null).getPipeline(),
            new ChannelSink() {
                public void eventSunk(ChannelPipeline pipeline, ChannelEvent e) {}
                public void exceptionCaught(ChannelPipeline pipeline, ChannelEvent e, ChannelPipelineException cause) {}
            }
        );
    }

    /**
     * @return the number of writes that have occurred.
     */
    public synchronized int getWriteCount() {
        return writtenObjects.size();
    }

    /**
     * Get the last object that was written to this channel.
     *
     * @return the object that was last written to this channel.
     */
    public synchronized Object getWrittenObject() {
        if (writtenObjects.size() == 0) {
            return null;
        }

        return writtenObjects.getLast();
    }

    /**
     * Return the list of objects that have been written to this
     * channel.
     */
    public synchronized List<Object> getObjects() {
        return writtenObjects;
    }

    @Override
    public synchronized ChannelFuture write(Object o) {
        SucceededChannelFuture future = new SucceededChannelFuture(this);
        writtenObjects.add(o);
        future.setSuccess();
        return future;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return null;
    }

    @Override
    public SocketChannelConfig getConfig() {
        return null;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return null;
    }

    @Override
    public boolean isBound() {
        return true;
    }

    @Override
    public boolean isConnected() {
        return true;
    }
}
