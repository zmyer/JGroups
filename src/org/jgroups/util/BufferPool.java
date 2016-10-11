package org.jgroups.util;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * @author Bela Ban
 * @since x.y
 */
public class BufferPool {
    protected final java.util.Queue<Buffer> pool=new ConcurrentLinkedQueue<>();

    public Buffer get(int size) {
        Buffer buf=pool.poll();
        if(buf == null || buf.getLength() < size)
            return new Buffer(new byte[size]);
        return buf;
    }

    public void release(Buffer buf) {
        if(buf != null)
            pool.offer(buf);
    }

    public int size() {return pool.size();}

    public String toString() {
        return pool.stream().map(Buffer::toString).collect(Collectors.joining(", "));
    }
}

