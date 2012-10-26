package org.cloudname.testtools.network;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TODO(borud): add Javadoc
 * TODO(borud): this class lacks unit tests
 *
 * @author dybdahl
 */
class ForwardThread extends Thread {
    private final static Logger log = Logger.getLogger(ClientThread.class.getName());

    private static final int BUFFER_SIZE = 4096;
    private InputStream inputStream;
    private OutputStream outputStream;
    private ClientThread parent;

    /**
     * TODO(borud): add Javadoc
     */
    public ForwardThread(ClientThread parent, InputStream inputStream, OutputStream outputStream) {
        this.parent = parent;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        start();
    }

    /**
     * TODO(borud): add Javadoc
     */
    public void run() {
        byte[] buffer = new byte[BUFFER_SIZE];
        try {
            while (true) {
                int bytesRead = inputStream.read(buffer);

                if (bytesRead == -1)
                    // End of stream is reached --> exit
                    break;

                outputStream.write(buffer, 0, bytesRead);
                outputStream.flush();
            }
        } catch (IOException e) {
            // Read/write failed --> connection is broken
            log.log(Level.SEVERE, "Forwarding in loop died.." + parent.toString(), e);
        }
        // Notify parent thread that the connection is broken
        parent.disconnect();
    }
}
