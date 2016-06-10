package org.jgroups.protocols;

/**
 * A bundler based on {@link org.jgroups.util.RingBuffer}
 * @author Bela Ban
 * @since  4.0
 */

import org.jgroups.Message;
import org.jgroups.util.RingBuffer;

/**
 * This bundler adds all (unicast or multicast) messages to a queue until max size has been exceeded, but does send
 * messages immediately when no other messages are available. https://issues.jboss.org/browse/JGRP-1540
 */
public class RingBufferBundler extends BaseBundler implements Runnable {
    protected RingBuffer<Message>    rb;
    protected volatile     Thread    bundler_thread;
    protected volatile boolean       running=true;
    protected int                    num_spins=40; // number of times we call Thread.yield before acquiring the lock (0 disables)
    protected static final String    THREAD_NAME="RingBufferBundler";

    public RingBufferBundler() {
    }

    protected RingBufferBundler(RingBuffer<Message> rb) {
        this.rb=rb;
    }

    public RingBufferBundler(int capacity) {
        this(new RingBuffer<>(assertPositive(capacity, "bundler capacity cannot be " + capacity)));
    }

    public Thread            getThread()               {return bundler_thread;}
    public int               getBufferSize()           {return rb.size();}
    public int               numSpins()                {return num_spins;}
    public RingBufferBundler numSpins(int n)           {num_spins=n; return this;}

    public void init(TP transport) {
        super.init(transport);
        if(rb == null)
            rb=new RingBuffer<>(assertPositive(transport.getBundlerCapacity(), "bundler capacity cannot be " + transport.getBundlerCapacity()));
    }

    public synchronized void start() {
        if(running)
            stop();
        bundler_thread=transport.getThreadFactory().newThread(this, THREAD_NAME);
        running=true;
        bundler_thread.start();
    }

    public synchronized void stop() {
        _stop(true);
    }

    public synchronized void stopAndFlush() {
        _stop(false);
    }

    public void send(Message msg) throws Exception {
        if(running)
            rb.put(msg);
    }

    public void run() {
        while(running) {
            try {
                readMessages();
            }
            catch(Throwable t) {
            }
        }
    }

    protected void readMessages() throws InterruptedException {
        int cnt=0, capacity=rb.capacity();
        int available_msgs=rb.waitForMessages(num_spins);
        int read_index=rb.readIndex();
        Object[] buf=rb.buf();

        for(int i=0; i < available_msgs; i++) {
            Message msg=(Message)buf[read_index];
            long size=msg.size();
            if(count + size >= transport.getMaxBundleSize())
                sendBundledMessages();
            addMessage(msg, size);
            buf[read_index]=null;
            if(++read_index == capacity)
                read_index=0;
            cnt++;
        }
        if(cnt > 0)
            rb.publishReadIndex(read_index, cnt);
        if(count > 0)
            sendBundledMessages();
    }

    protected void _stop(boolean clear_queue) {
        running=false;
        Thread tmp=bundler_thread;
        bundler_thread=null;
        if(tmp != null) {
            tmp.interrupt();
            if(tmp.isAlive()) {
                try {tmp.join(500);} catch(InterruptedException e) {}
            }
        }
        if(clear_queue)
            rb.clear();
    }


    protected static int assertPositive(int value, String message) {
        if(value <= 0) throw new IllegalArgumentException(message);
        return value;
    }
}
