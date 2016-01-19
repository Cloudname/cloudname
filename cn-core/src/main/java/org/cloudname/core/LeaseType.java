package org.cloudname.core;

/**
 * Lease type. There are two kinds of leases:
 * <ul>
 *     <li>PERMANENT leases which will linger around forever until removed by some client.</li>
 *     <li>TEMPORARY leases which will only exist as long as the client is connected to the
 *     backend.</li>
 * </ul>
 */
public enum LeaseType {
    PERMANENT,
    TEMPORARY
}
