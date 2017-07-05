package org.jgroups.util;

import java.util.Collection;
import java.util.Map;
import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.LazyRemovalCache;

/**
 * Maintains the mappings between addresses and logical names (moved out of UUID.cache into a
 * separate class)
 *
 * @author Bela Ban
 * @since 4.0
 */
// TODO: 17/7/5 by zmyer
public class NameCache {

    /** Keeps track of associations between addresses and logical names */
    protected static final LazyRemovalCache<Address, String> cache;

    private static final LazyRemovalCache.Printable<Address, LazyRemovalCache.Entry<String>> print_function
        = (key, entry) -> entry.getVal() + ": " + (key instanceof UUID ? ((UUID) key).toStringLong() : key) + "\n";

    static {
        int max_elements = 500;
        long max_age = 5000L;

        try {
            String tmp = Util.getProperty(new String[] {Global.NAME_CACHE_MAX_ELEMENTS}, null, null, "500");
            if (tmp != null)
                max_elements = Integer.valueOf(tmp);
        } catch (Throwable ignored) {
        }

        try {
            String tmp = Util.getProperty(new String[] {Global.NAME_CACHE_MAX_AGE}, null, null, "120000");
            if (tmp != null)
                max_age = Long.valueOf(tmp);
        } catch (Throwable ignored) {
        }

        cache = new LazyRemovalCache<>(max_elements, max_age);
    }

    public static void add(Address uuid, String logical_name) {
        cache.add(uuid, logical_name); // overwrite existing entry
    }

    public static void add(Map<Address, String> map) {
        if (map == null)
            return;
        for (Map.Entry<Address, String> entry : map.entrySet())
            add(entry.getKey(), entry.getValue());
    }

    public static String get(Address logical_addr) {
        return cache.get(logical_addr);
    }

    /** Returns a <em>copy</em> of the cache's contents */
    public static Map<Address, String> getContents() {
        return cache.contents();
    }

    public static void remove(Address addr) {
        cache.remove(addr);
    }

    public static void removeAll(Collection<Address> mbrs) {
        cache.removeAll(mbrs);
    }

    public static void retainAll(Collection<Address> logical_addrs) {
        cache.retainAll(logical_addrs);
    }

    public static String printCache() {
        return cache.printCache(print_function);
    }

}
