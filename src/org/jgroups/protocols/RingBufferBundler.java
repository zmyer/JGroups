package org.jgroups.protocols;

/**
 * A bundler based on {@link org.jgroups.util.RingBuffer}
 * @author Bela Ban
 * @since  4.0
 */

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.util.RingBuffer;
import org.jgroups.util.Util;

import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

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
    protected Consumer<Integer>      wait_strategy=YIELD;

    protected static final Consumer<Integer> SPIN=it -> {;};
    protected static final Consumer<Integer> YIELD=it -> Thread.yield();
    protected static final Consumer<Integer> PARK=it -> LockSupport.parkNanos(1);
    protected static final Consumer<Integer> SPIN_YIELD_PARK=it -> {
        if(it < 10)
            ; // spin
        else if(it < 20)
            Thread.yield();
        else
            LockSupport.parkNanos(1);
    };

    public RingBufferBundler() {
    }

    protected RingBufferBundler(RingBuffer<Message> rb) {
        this.rb=rb;
    }

    public RingBufferBundler(int capacity) {
        this(new RingBuffer<>(Message.class, assertPositive(capacity, "bundler capacity cannot be " + capacity)));
    }

    public RingBuffer<Message> buf()                     {return rb;}
    public Thread              getThread()               {return bundler_thread;}
    public int                 getBufferSize()           {return rb.size();}
    public int                 numSpins()                {return num_spins;}
    public RingBufferBundler   numSpins(int n)           {num_spins=n; return this;}
    public String              waitStrategy()            {return print(wait_strategy);}
    public RingBufferBundler   waitStrategy(String st)   {wait_strategy=createWaitStrategy(st); return this;}


    public void init(TP transport) {
        super.init(transport);
        if(rb == null)
            rb=new RingBuffer<>(Message.class, assertPositive(transport.getBundlerCapacity(), "bundler capacity cannot be " + transport.getBundlerCapacity()));
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
        int capacity=rb.capacity();
        int available_msgs=rb.waitForMessages(num_spins, wait_strategy);
        int read_index=rb.readIndex();
        Message[] buf=rb.buf();
        sendBundledMessages(buf, read_index, available_msgs, capacity);
        rb.publishReadIndex(available_msgs);
    }



    /** Read and send messages in range [read-index .. read-index+available_msgs-1] */
    public void sendBundledMessages(final Message[] buf, final int read_index, final int available_msgs, final int capacity) {
        int       max_bundle_size=transport.getMaxBundleSize();
        byte[]    cluster_name=transport.cluster_name.chars();
        int       start=read_index;
        final int end=index(start + available_msgs-1, capacity); // index of the last message to be read

        for(;;) {
            Message msg=buf[start];
            if(msg == null) {
                if(start == end)
                    break;
                start=advance(start, capacity);
                continue;
            }

            Address dest=msg.dest();
            int num_msgs=1;

            // iterate through the following messages and find messages to the same destination
            count=msg.size();
            int i=start;
            while(i != end) {
                i=advance(i, capacity);
                Message next=buf[i];
                if(next != null && (dest == next.getDest() || (dest != null && dest.equals(next.dest())))) {
                    next.dest(dest); // avoid further equals() calls
                    long size=next.size();
                    if(count + size > max_bundle_size)
                        break;
                    count+=size;
                    num_msgs++;
                }
            }

            try {
                output.position(0);
                if(num_msgs == 1) {
                    // System.out.printf("single msg to %s (count=%d)\n", msg.dest(), count);
                    sendSingleMessage(msg);
                    buf[start]=null;
                }
                else {
                    // System.out.printf("message bundle of %d to %s (count=%d)\n", num_msgs, dest, count);
                    Util.writeMessageListHeader(dest, msg.src(), cluster_name, num_msgs, output, dest == null);
                    i=start;
                    while(num_msgs > 0) {
                        Message next=buf[i];
                        // since we assigned the matching destination we can do plain ==
                        if(next != null && next.dest() == dest) {
                            next.writeToNoAddrs(next.src(), output, transport.getId());
                            buf[i]=null;
                            num_msgs--;
                        }
                        if(i == end)
                            break;
                        i=advance(i, capacity);
                    }
                    transport.doSend(output.buffer(), 0, output.position(), dest);
                }
            }
            catch(Exception ex) {
                log.error("failed to send message", ex);
            }

            if(start == end)
                break;
            start=advance(start, capacity);
        }
    }


    protected static final int advance(int index, int capacity) { // should be inlined
        return index+1 == capacity? 0 : index+1;
    }

    // fast equivalent to %
    protected static int index(int idx, int capacity) {
        return idx & (capacity-1);
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

    protected static String print(Consumer<Integer> wait_strategy) {
        if(wait_strategy      == null)            return null;
        if(wait_strategy      == SPIN)            return "spin";
        else if(wait_strategy == YIELD)           return "yield";
        else if(wait_strategy == PARK)            return "park";
        else if(wait_strategy == SPIN_YIELD_PARK) return "spin-yield-park";
        else return wait_strategy.getClass().getSimpleName();
    }

    protected Consumer<Integer> createWaitStrategy(String st) {
        if(st == null) return null;
        switch(st) {
            case "spin":            return wait_strategy=SPIN;
            case "yield":           return wait_strategy=YIELD;
            case "park":            return wait_strategy=PARK;
            case "spin_yield_park": return wait_strategy=SPIN_YIELD_PARK;
            default:
                try {
                    Class<Consumer<Integer>> clazz=Util.loadClass(st, this.getClass());
                    return clazz.newInstance();
                }
                catch(Throwable t) {
                    log.error("failed creating wait_strategy " + st, t);
                    return null;
                }
        }
    }

    protected static int assertPositive(int value, String message) {
        if(value <= 0) throw new IllegalArgumentException(message);
        return value;
    }
}
