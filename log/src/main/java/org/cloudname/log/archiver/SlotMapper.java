package org.cloudname.log.archiver;

import java.io.File;
import java.util.TimeZone;
import java.util.Calendar;
import java.util.GregorianCalendar;

import java.util.Map;
import java.util.HashMap;

import java.util.logging.Logger;


/**
 * This class implements a utility for mapping a timestamp to a given
 * hour slot.  An hour slot is a path fragment that can be used to
 * construct a filesystem path.
 *
 * <p> This implementation makes the assumption that the timestamps
 * that need to be mapped will usually be fairly close to each other
 * with the occasional outlier.  To speed up mapping the
 * implementation makes use of an internal cache that is occasionally
 * nuked completely.  For all but the most degenerate cases this
 * should offer a considerable speedup.
 *
 * <p> If in your logs you find that the code is purging the slot
 * cache many times per second you might want to up the value of
 * LARGE_CACHE, but I doubt that this would ever occur under typical
 * workloads (famous last words).
 *
 * <p>
 * This class is not thread-safe.  Or rather, it is only mildly
 * thread-un-safe :-).
 *
 * @author borud
 */
public class SlotMapper {
    // All formatted times are UTC always.
    public static final TimeZone TZ = TimeZone.getTimeZone("UTC");

    // This is the slot length in milliseconds. A slot is one hour.
    public static final long SLOT_LENGTH = 3600000L;

    // The max size we allow the cache to grow to before purging it.
    public static final int LARGE_CACHE = 255;

    private final Map<Long,String> slotCache = new HashMap<Long,String>();

    /**
     * Map a timestamp to a slot.  Uses a cache to speed up mapping.
     *
     * @param time milliseconds since epoch as returned by
     *   System.currentTimeMillis().
     * @param service name of the service. Will be prepended to file name.
     * @return the path of the slot we wish to map to
     */
    public String map(long time, String service)
    {
        // I am going out on a limb here assuming that if we come across
        // log messages that come from before 1970 you won't be too upset
        // about me blatantly assuming they may be a bit fishy.
        if (time < 0) {
            throw new IllegalArgumentException("Timestamp was less than zero");
        }

        long slotnum = time / SLOT_LENGTH;
        String slot = slotCache.get(slotnum);

        // If a slot was not found we calculate the path for that slot
        // and add it to the cache
        if (null == slot) {
            // Take this opportunity to possibly nuke the entire cache
            // if it has grown beyond LARGE_CACHE number of entries.
            // For all but the most degenerate use-cases this should not
            // affect performance noticably
            if (slotCache.size() > LARGE_CACHE) {
                slotCache.clear();
            }

            slot = mapToPath(time, service);
            slotCache.put(slotnum, slot);
        }

        return slot;
    }

    /**
     * Map a timestamp to a given hour slot and return a path
     * substring for a slot representing that hour.  Note that the
     * separator character used in the path is the same as
     * File.separator so the mapping should work on Windows
     * filesystems as well.
     *
     * @param time the time as returned by System.currentTimeMillis().
     * @param service name of the service. Will be appended to file name.
     * @return the path of the slot.
     */
    public static String mapToPath(long time, String service)
    {
        GregorianCalendar calendar = new GregorianCalendar(TZ);
        calendar.setTimeInMillis(time);

        // Fetch the various fields.  Note that the month is 0-based.
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);

        String monthStr = (month < 10) ? "0" + month : month + "";
        String dayStr   = (day < 10) ? "0" + day : day + "";

        return new StringBuilder(16)
            .append(year)
            .append(File.separator)
            .append(monthStr)
            .append(File.separator)
            .append(dayStr)
            .append(File.separator)

            // Append the name of the service in filename
            .append(service).append("_")

            // Repeat ISO date in filename
            .append(year).append("-").append(monthStr).append("-").append(dayStr)
            .append("_")

            .append((hour < 10) ? "0" + hour : hour)
            .toString();
    }
}
