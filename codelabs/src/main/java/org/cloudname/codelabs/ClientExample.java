package org.cloudname.codelabs;

import org.cloudname.Cloudname;
import org.cloudname.CloudnameException;
import org.cloudname.Endpoint;
import org.cloudname.Resolver;
import org.cloudname.zk.ZkCloudname;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

/**
 * Simple wget that pulls a string from a server specified by coordinate + endpoint.
 * @author dybdahl
 */
public class ClientExample {

    /**
     * Args has one parameter that is instance number.
     * @param args
     */
    public static void main(String[] args) throws IOException, CloudnameException {
        Cloudname cloudName = new ZkCloudname.Builder().setConnectString("127.0.0.1:5454").build().connect();
        Resolver resolver = cloudName.getResolver();

        List<Endpoint> endpoints = resolver.resolve(String.format("info.%s.hello.somebody.aa", args[0]));
        if (endpoints.size() != 1) {
            System.err.println("Did not resolve endpoint correctly, something went wrong.");
            return;
        }

        String url = "http://" + endpoints.get(0).getHost() + ":" + endpoints.get(0).getPort() + "/info";
        URL u = new URL(url);
        InputStream is = u.openStream();
        DataInputStream dis = new DataInputStream(new BufferedInputStream(is));
        byte[] infoString = new byte[1000];
        dis.read(infoString);
        String resultString = new String(infoString);
        System.out.println("Got string from server: " + resultString);
        is.close();
    }
}