package org.jgroups.protocols;

/**
 * A bundler based on {@link org.jgroups.util.RingBuffer}
 * @author Bela Ban
 * @since  4.0
 */

import org.jgroups.Message;
import org.jgroups.util.RingBuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * This bundler adds all (unicast or multicast) messages to a queue until max size has been exceeded, but does send
 * messages immediately when no other messages are available. https://issues.jboss.org/browse/JGRP-1540
 */
public class RingBufferBundler extends BaseBundler implements Runnable {
    protected RingBuffer<Message>    buf;
    protected List<Message>          remove_queue;
    protected volatile     Thread    bundler_thread;
    protected volatile boolean       running=true;
    protected int                    num_spins=40; // number of times we call Thread.yield before acquiring the lock (0 disables)
    protected static final String    THREAD_NAME="RingBufferBundler";

    public RingBufferBundler() {
        this.remove_queue=new ArrayList<>(16);
    }

    protected RingBufferBundler(RingBuffer<Message> buf) {
        this.buf=buf;
        this.remove_queue=new ArrayList<>(16);
    }

    public RingBufferBundler(int capacity) {
        this(new RingBuffer<>(assertPositive(capacity, "bundler capacity cannot be " + capacity)));
    }

    public Thread            getThread()               {return bundler_thread;}
    public int               getBufferSize()           {return buf.size();}
    public int               removeQueueSize()         {return remove_queue.size();}
    public RingBufferBundler removeQueueSize(int size) {this.remove_queue=new ArrayList<>(size); return this;}
    public int               numSpins()                {return num_spins;}
    public RingBufferBundler numSpins(int n)           {num_spins=n; return this;}

    public void init(TP transport) {
        super.init(transport);
        if(buf == null)
            buf=new RingBuffer<>(assertPositive(transport.getBundlerCapacity(), "bundler capacity cannot be " + transport.getBundlerCapacity()));
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
            buf.put(msg);
    }

    public void run() {
        while(running) {
            try {
                remove_queue.clear();
                // int num_msgs=buf.drainTo(remove_queue, Integer.MAX_VALUE, true);
                int num_msgs=buf.drainToLockless(remove_queue, Integer.MAX_VALUE, true, num_spins);
                if(num_msgs <= 0)
                    continue;
                for(int i=0; i < remove_queue.size(); i++) {
                    Message msg=remove_queue.get(i);
                    long size=msg.size();
                    if(count + size >= transport.getMaxBundleSize())
                        sendBundledMessages();
                    addMessage(msg, size);
                }
                if(count > 0)
                    sendBundledMessages();
            }
            catch(Throwable t) {
            }
        }
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
            buf.clear();
    }


    protected static int assertPositive(int value, String message) {
        if(value <= 0) throw new IllegalArgumentException(message);
        return value;
    }
}
