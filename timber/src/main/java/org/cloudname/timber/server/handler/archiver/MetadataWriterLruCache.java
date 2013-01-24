package org.cloudname.timber.server.handler.archiver;

import java.io.*;
import java.util.*;

public class MetadataWriterLruCache<K,V> extends LinkedHashMap<K,V> {
    private final int capacity;

    public MetadataWriterLruCache(int capacity)
    {
        super(capacity, (float) 0.75, true);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K,V> eldest)
    {
        if (size() > capacity) {
            BufferedWriter writer = (BufferedWriter) eldest.getValue();
            try {
                writer.close();
            } catch (IOException e) {
                // Log the error and ignore it
            }
            return true;
        }

        return false;
    }
}