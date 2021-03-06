package org.jgroups.blocks;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jgroups.util.Util;

/**
 * Cache which doesn't remove elements on remove(), removeAll() or retainAll(), but only removes
 * elements when a configurable size limit has been exceeded. In that case, all elements marked as
 * removable and older than a configurable time are evicted. Elements are marked as removable by
 * remove(), removeAll() and retainAll(). When an elements is marked as removable, but later
 * reinserted, the mark is removed.
 *
 * @author Bela Ban
 */
// TODO: 17/7/4 by zmyer
public class LazyRemovalCache<K, V> {
    private final ConcurrentMap<K, Entry<V>> map = Util.createConcurrentMap();

    /**
     * Max number of elements, if exceeded, we remove all elements marked as removable and older
     * than max_age ms
     */
    private final int max_elements;
    private final long max_age; // ns

    public interface Printable<K, V> {
        String print(K key, V entry);
    }

    LazyRemovalCache() {
        this(200, 5000L);
    }

    /**
     * Creates a new instance
     *
     * @param max_elements The max number of elements in the cache
     * @param max_age The max age (in ms) an entry can have before it is considered expired (and can
     * be removed on the next sweep)
     */
    public LazyRemovalCache(int max_elements, long max_age) {
        this.max_elements = max_elements;
        this.max_age = TimeUnit.NANOSECONDS.convert(max_age, TimeUnit.MILLISECONDS);
    }

    public boolean add(K key, V val) {
        boolean added = false;
        if (key != null && val != null)
            added = map.put(key, new Entry<>(val)) == null; // overwrite existing element (new timestamp, and possible removable mark erased)
        checkMaxSizeExceeded();
        return added;
    }

    public void addAll(Map<K, V> m) {
        if (m == null)
            return;
        for (Map.Entry<K, V> entry : m.entrySet()) {
            K key = entry.getKey();
            V val = entry.getValue();
            map.put(key, new Entry<>(val));
        }
    }

    public Set<Map.Entry<K, Entry<V>>> entrySet() {
        return map.entrySet();
    }

    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    /**
     * Returns true if all of the keys in keys are present. Returns false if one or more of the keys
     * are absent
     */
    public boolean containsKeys(Collection<K> keys) {
        for (K key : keys)
            if (!map.containsKey(key))
                return false;
        return true;
    }

    public V get(K key) {
        if (key == null)
            return null;
        Entry<V> entry = map.get(key);
        return entry != null ? entry.val : null;
    }

    public K getByValue(V val) {
        if (val == null)
            return null;
        for (Map.Entry<K, Entry<V>> entry : map.entrySet()) {
            Entry<V> v = entry.getValue();
            if (Objects.equals(v.val, val))
                return entry.getKey();
        }
        return null;
    }

    public void remove(K key) {
        remove(key, false);
    }

    public void remove(K key, boolean force) {
        if (key == null)
            return;
        if (force)
            map.remove(key);
        else {
            Entry<V> entry = map.get(key);
            if (entry != null)
                entry.setRemovable(true);
        }
        checkMaxSizeExceeded();
    }

    public void removeAll(Collection<K> keys) {
        removeAll(keys, false);
    }

    public void removeAll(Collection<K> keys, boolean force) {
        if (keys == null || keys.isEmpty())
            return;
        if (force)
            map.keySet().removeAll(keys);
        else {
            for (K key : keys) {
                Entry<V> entry = map.get(key);
                if (entry != null)
                    entry.setRemovable(true);
            }
        }
        checkMaxSizeExceeded();
    }

    public void clear(boolean force) {
        if (force)
            map.clear();
        else {
            for (Map.Entry<K, Entry<V>> entry : map.entrySet()) {
                Entry<V> val = entry.getValue();
                if (val != null) {
                    Entry<V> tmp = entry.getValue();
                    if (tmp != null)
                        tmp.setRemovable(true);
                }
            }
        }
    }

    public void retainAll(Collection<K> keys) {
        retainAll(keys, false);
    }

    public void retainAll(Collection<K> keys, boolean force) {
        if (keys == null || keys.isEmpty())
            return;
        if (force)
            map.keySet().retainAll(keys);
        else {
            map.entrySet().stream().filter(entry -> !keys.contains(entry.getKey()))
                .forEach(entry -> {
                    Entry<V> val = entry.getValue();
                    if (val != null)
                        val.setRemovable(true);
                });
        }

        // now make sure that all elements in keys have removable=false
        for (K key : keys) {
            Entry<V> val = map.get(key);
            if (val != null && val.removable)
                val.setRemovable(false);
        }

        checkMaxSizeExceeded();
    }

    public Set<K> keySet() {
        return map.keySet();
    }

    public Set<V> values() {
        return map.values().stream().map(entry -> entry.val).collect(Collectors.toSet());
    }

    Iterable<Entry<V>> valuesIterator() {
        return () -> map.values().iterator();
    }

    /**
     * Adds all value which have not been marked as removable to the returned set
     */
    public Set<V> nonRemovedValues() {
        return map.values().stream().filter(entry -> !entry.removable)
            .map(entry -> entry.val).collect(Collectors.toSet());
    }

    public Map<K, V> contents() {
        return contents(false);
    }

    public Map<K, V> contents(boolean skip_removed_values) {
        Map<K, V> retval = new HashMap<>();
        for (Map.Entry<K, Entry<V>> entry : map.entrySet()) {
            Entry<V> val = entry.getValue();
            if (val.isRemovable() && skip_removed_values)
                continue;
            retval.put(entry.getKey(), entry.getValue().val);
        }
        return retval;
    }

    public int size() {
        return map.size();
    }

    public String printCache() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<K, Entry<V>> entry : map.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    public String printCache(Printable print_function) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<K, Entry<V>> entry : map.entrySet()) {
            K key = entry.getKey();
            sb.append(print_function.print(key, entry.getValue()));
        }
        return sb.toString();
    }

    public String toString() {
        return printCache();
    }

    private void checkMaxSizeExceeded() {
        if (map.size() > max_elements)
            removeMarkedElements(false);
    }

    /**
     * Removes elements marked as removable
     *
     * @param force If set to true, all elements marked as 'removable' will get removed, regardless
     * of expiration
     */
    public void removeMarkedElements(boolean force) {
        long curr_time = System.nanoTime();
        for (Iterator<Map.Entry<K, Entry<V>>> it = map.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<K, Entry<V>> entry = it.next();
            Entry<V> tmp = entry.getValue();
            if (tmp == null)
                continue;
            if (tmp.removable && (force || (curr_time - tmp.timestamp) >= max_age)) {
                it.remove();
            }
        }
    }

    /**
     * Removes elements marked as removable
     */
    void removeMarkedElements() {
        removeMarkedElements(false);
    }

    public static class Entry<V> {
        protected final V val;
        protected long timestamp = System.nanoTime();
        boolean removable = false;

        public Entry(V val) {
            this.val = val;
        }

        boolean isRemovable() {
            return removable;
        }

        void setRemovable(boolean flag) {
            if (this.removable != flag) {
                this.removable = flag;
                timestamp = System.nanoTime();
            }
        }

        public V getVal() {
            return val;
        }

        public String toString() {
            return toString(null);
        }

        public String toString(Function<V, String> print_val) {
            StringBuilder sb = new StringBuilder(print_val != null ?
                print_val.apply(val) : val.toString()).append(" (");
            long age = TimeUnit.MILLISECONDS.convert(System.nanoTime() - timestamp,
                TimeUnit.NANOSECONDS);
            if (age < 1000)
                sb.append(age).append(" ms");
            else
                sb.append(TimeUnit.SECONDS.convert(age, TimeUnit.MILLISECONDS)).append(" secs");
            sb.append(" old").append((removable ? ", removable" : "")).append(")");
            return sb.toString();
        }
    }
}
