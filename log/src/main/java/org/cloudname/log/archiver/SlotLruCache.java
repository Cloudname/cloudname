package org.cloudname.log.archiver;

import java.io.IOException;

import java.util.Map;
import java.util.LinkedHashMap;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 *
 * @author borud
 */
public class SlotLruCache<K,V> extends LinkedHashMap<K,V> {
    private final int capacity;

    public SlotLruCache(int capacity)
    {
        super(capacity, (float) 0.75, true);
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K,V> eldest)
    {
        if (size() > capacity) {
            Slot slot = (Slot) eldest.getValue();
            try {
                slot.close();
            } catch (IOException e) {
                // Log the error and ignore it
            }
            return true;
        }

        return false;
    }
}
