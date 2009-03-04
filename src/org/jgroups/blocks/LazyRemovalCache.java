package org.jgroups.blocks;

import java.util.Collection;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Cache which doesn't remove elements on remove(), removeAll() or retainAll(), but only removes elements when a
 * configurable size limit has been exceeded. In that case, all elements marked as removable and older than a
 * configurable time are evicted. Elements are marked as removable by remove(), removeAll() and retainAll(). When
 * an elements is marked as removable, but later reinserted, the mark is removed.
 * @author Bela Ban
 * @version $Id: LazyRemovalCache.java,v 1.1.2.1 2009/03/04 10:04:38 belaban Exp $
 */
public class LazyRemovalCache<K,V> {
    private final ConcurrentMap<K,Entry<V>> map=new ConcurrentHashMap<K,Entry<V>>();

    /** Max number of elements, if exceeded, we remove all elements marked as removable and older than max_age ms */
    private final int max_elements;

    private final long max_age;


    public LazyRemovalCache() {
        this(200, 5000L);
    }

    public LazyRemovalCache(int max_elements, long max_age) {
        this.max_elements=max_elements;
        this.max_age=max_age;
    }

    public void add(K key, V val) {
        if(key != null && val != null)
            map.put(key, new Entry<V>(val)); // overwrite existing element (new timestamp, and possible removable mark erased)
        checkMaxSizeExceeded();
    }

    public V get(K key) {
        if(key == null)
            return null;
        Entry<V> entry=map.get(key);
        return entry != null? entry.val : null;
    }

    public void remove(K key) {
        remove(key, false);
    }

    public void remove(K key, boolean force) {
        if(key == null)
            return;
        if(force)
            map.remove(key);
        else {
            Entry<V> entry=map.get(key);
            entry.removable=true;
        }
    }

    public void removeAll(Collection<K> keys) {
        removeAll(keys, false);
    }

    public void removeAll(Collection<K> keys, boolean force) {
        if(keys == null || keys.isEmpty())
            return;
        if(force)
            map.keySet().removeAll(keys);
        else {
            for(K key: keys) {
                Entry<V> entry=map.get(key);
                entry.removable=true;
            }
        }
    }

    public void retainAll(Collection<K> keys) {
        retainAll(keys, false);
    }

    public void retainAll(Collection<K> keys, boolean force) {
        if(keys == null || keys.isEmpty())
            return;
        if(force)
            map.keySet().retainAll(keys);
        else {
            for(Map.Entry<K,Entry<V>> entry: map.entrySet()) {
                if(!keys.contains(entry.getKey())) {
                    entry.getValue().removable=true;
                }
            }
        }
    }

    public int size() {
        return map.size();
    }

    public String printCache() {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<K,Entry<V>> entry: map.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        return sb.toString();
    }

    public String toString() {
        return printCache();
    }


    private void checkMaxSizeExceeded() {
        if(map.size() >= max_elements) {
            removeMarkedElements();
        }
    }

    /**
     * Removes elements marked as removable
     */
    public void removeMarkedElements() {
        for(Iterator<Map.Entry<K,Entry<V>>> it=map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<K, Entry<V>> entry=it.next();
            Entry<V> tmp=entry.getValue();
            if(tmp.removable) {
                it.remove();
            }
        }
    }


    private static class Entry<V> {
        private final V val;
        private final long timestamp=System.currentTimeMillis();
        private boolean removable=false;

        public Entry(V val) {
            this.val=val;
        }

        public String toString() {
            return val + " (" + (System.currentTimeMillis() - timestamp) + "ms old" + (removable? ", removable" : "") + ")";
        }
    }
}
