package org.jgroups.protocols;

/**
 * A bundler based on a lockless ring buffer
 * @author Bela Ban
 * @since  4.0
 */

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.util.Util;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

/**
 * This bundler adds all (unicast or multicast) messages to a ring buffer until max size has been exceeded, but does
 * send messages immediately when no other messages are available. If no space is available, a message by a sender
 * thread is simply dropped, as it will get retransmitted anyway. This makes this implementation completely non-blocking.
 * https://issues.jboss.org/browse/JGRP-1540
 */
public class RingBufferBundlerLockless2 extends BaseBundler implements Runnable {
    protected Message[]                   buf;
    protected volatile int                read_index;
    protected volatile int                write_index=1;
    protected final AtomicLong            accumulated_bytes=new AtomicLong(0);
    protected final AtomicInteger         num_threads=new AtomicInteger(0);

    protected volatile Thread             bundler_thread;
    protected volatile boolean            running=true;
    protected int                         num_spins=40; // number of times we call Thread.yield before acquiring the lock (0 disables)
    protected static final String         THREAD_NAME="RingBufferBundlerLockless2";
    protected BiConsumer<Integer,Integer> wait_strategy=SPIN_PARK;
    protected static final AtomicIntegerFieldUpdater write_updater;
    protected static final Message        NULL_MSG=new Message(false);
    protected static final BiConsumer<Integer,Integer> SPIN=(it,spins) -> {;};
    protected static final BiConsumer<Integer,Integer> YIELD=(it,spins) -> Thread.yield();
    protected static final BiConsumer<Integer,Integer> PARK=(it,spins) -> LockSupport.parkNanos(1);
    protected static final BiConsumer<Integer,Integer> SPIN_PARK=(it, spins) -> {
        if(it < spins/10)
            ; // spin for the first 10% of all iterations, then switch to park()
        LockSupport.parkNanos(1);
    };
    protected static final BiConsumer<Integer,Integer> SPIN_YIELD=(it, spins) -> {
        if(it < spins/10)
            ;           // spin for the first 10% of the total number of iterations
        Thread.yield(); //, then switch to yield()
    };

    static {
        //noinspection AtomicFieldUpdaterIssues
        write_updater=AtomicIntegerFieldUpdater.newUpdater(RingBufferBundlerLockless2.class, "write_index");
    }


    public RingBufferBundlerLockless2() {
        this(1024);
    }


    public RingBufferBundlerLockless2(int capacity) {
        buf=new Message[Util.getNextHigherPowerOfTwo(capacity)]; // for efficient % (mod) op
    }

    public int                        readIndex()             {return read_index;}
    public int                        writeIndex()            {return write_index;}
    public RingBufferBundlerLockless2 reset()                 {read_index=0; write_index=1; return this;}
    public Thread                     getThread()             {return bundler_thread;}
    public int                        numSpins()              {return num_spins;}
    public RingBufferBundlerLockless2 numSpins(int n)         {num_spins=n; return this;}
    public String                     waitStrategy()          {return print(wait_strategy);}
    public RingBufferBundlerLockless2 waitStrategy(String st) {wait_strategy=createWaitStrategy(st, YIELD); return this;}

    public int getBufferSize() {
        int ri=read_index, wi=write_index;
        return ri < wi? wi-ri-1 : buf.length - ri -1 +wi;
    }


    public void init(TP transport) {
        super.init(transport);
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
        if(msg == null)
            throw new IllegalArgumentException("message must not be null");
        if(!running)
            return;

        num_threads.incrementAndGet();

        int tmp_write_index=getWriteIndex(read_index);
        // System.out.printf("[%d] tmp_write_index=%d\n", Thread.currentThread().getId(), tmp_write_index);
        if(tmp_write_index == -1) {
            System.err.printf("buf is full: %s\n", toString()); //todo: change to log stmt
            unparkIfNeeded(0);
            return;
        }

        buf[tmp_write_index]=msg;
        unparkIfNeeded(msg.size());
    }

    protected void unparkIfNeeded(long size) {
        long acc_bytes=size > 0? accumulated_bytes.addAndGet(size) : accumulated_bytes.get();
        boolean no_other_threads=num_threads.decrementAndGet() == 0;

        boolean unpark=(acc_bytes >= transport.getMaxBundleSize() && accumulated_bytes.compareAndSet(acc_bytes, 0))
          ||  no_other_threads;

        // only 2 threads at a time should do this in parallel (1st cond and 2nd cond)
        if(unpark)
            LockSupport.unpark(bundler_thread);
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

    protected int getWriteIndex(int current_read_index) {
        for(;;) {
            int wi=write_index;
            int next_wi=index(wi+1);
            if(next_wi == current_read_index)
                return -1;
            if(write_updater.compareAndSet(this, wi, next_wi))
                return wi;
        }
    }



    public int _readMessages() throws InterruptedException {
        int ri=read_index;
        int wi=write_index;

        if(index(ri+1) == wi)
            return 0;

        int sent_msgs=sendBundledMessages(buf, ri, wi);
        read_index=index(ri + sent_msgs); // publish read_index to main memory
        // todo: if writers are blocked -> signalAll() on a not_full condition
        return sent_msgs;
    }



    protected void readMessages() throws InterruptedException {
        _readMessages();
        LockSupport.park();
    }



    /** Read and send messages in range [read-index+1 .. write_index-1] */
    public int sendBundledMessages(final Message[] buf, final int read_index, final int write_index) {
        int       max_bundle_size=transport.getMaxBundleSize();
        byte[]    cluster_name=transport.cluster_name.chars();
        int       sent_msgs=0;

        for(int i=increment(read_index); i != write_index; i=increment(i)) {
            Message msg=buf[i];
            if(msg == NULL_MSG) {
                buf[i]=null;
                continue;
            }
            if(msg == null)
                break;

            Address dest=msg.dest();
            try {
                output.position(0);
                Util.writeMessageListHeader(dest, msg.src(), cluster_name, 1, output, dest == null);

                // remember the position at which the number of messages (an int) was written, so we can later set the
                // correct value (when we know the correct number of messages)
                int size_pos=output.position() - Global.INT_SIZE;
                int num_msgs=marshalMessagesToSameDestination(dest, buf, i, write_index, max_bundle_size);
                if(num_msgs > 0)
                    buf[i]=null;
                sent_msgs+=num_msgs;
                int current_pos=output.position();
                output.position(size_pos);
                output.writeInt(num_msgs);
                output.position(current_pos);
                transport.doSend(output.buffer(), 0, output.position(), dest);
            }
            catch(Exception ex) {
                log.error("failed to send message(s)", ex);
            }
        }
        return sent_msgs;
    }

    public String toString() {
        return String.format("read-index=%d write-index=%d size=%d cap=%d\n",
                             read_index, write_index, getBufferSize(), buf.length);
    }



    // Iterate through the following messages and find messages to the same destination (dest) and write them to output
    protected int marshalMessagesToSameDestination(Address dest, Message[] buf, final int start_index, final int end_index,
                                                   int max_bundle_size) throws Exception {
        int num_msgs=0, bytes=0;
        for(int i=start_index; i != end_index; i=increment(i)) {
            Message msg=buf[i];
            if(msg != null && msg != NULL_MSG && Objects.equals(dest, msg.dest())) {
                long msg_size=msg.size();
                if(bytes + msg_size > max_bundle_size)
                    break;
                bytes+=msg_size;
                num_msgs++;
                buf[i]=NULL_MSG;
                msg.writeToNoAddrs(msg.src(), output, transport.getId());
            }
        }
        return num_msgs;
    }

    protected final int increment(int index) {return index+1 == buf.length? 0 : index+1;}
    protected final int index(int idx)     {return idx & (buf.length-1);}    // fast equivalent to %

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
            reset();
    }

    protected static String print(BiConsumer<Integer,Integer> wait_strategy) {
        if(wait_strategy      == null)            return null;
        if(wait_strategy      == SPIN)            return "spin";
        else if(wait_strategy == YIELD)           return "yield";
        else if(wait_strategy == PARK)            return "park";
        else if(wait_strategy == SPIN_PARK)       return "spin-park";
        else if(wait_strategy == SPIN_YIELD)      return "spin-yield";
        else return wait_strategy.getClass().getSimpleName();
    }

    protected BiConsumer<Integer,Integer> createWaitStrategy(String st, BiConsumer<Integer,Integer> default_wait_strategy) {
        if(st == null) return default_wait_strategy != null? default_wait_strategy : null;
        switch(st) {
            case "spin":            return wait_strategy=SPIN;
            case "yield":           return wait_strategy=YIELD;
            case "park":            return wait_strategy=PARK;
            case "spin_park":
            case "spin-park":       return wait_strategy=SPIN_PARK;
            case "spin_yield":
            case "spin-yield":      return wait_strategy=SPIN_YIELD;
            default:
                try {
                    Class<BiConsumer<Integer,Integer>> clazz=Util.loadClass(st, this.getClass());
                    return clazz.newInstance();
                }
                catch(Throwable t) {
                    log.error("failed creating wait_strategy " + st, t);
                    return default_wait_strategy != null? default_wait_strategy : null;
                }
        }
    }

    protected static int assertPositive(int value, String message) {
        if(value <= 0) throw new IllegalArgumentException(message);
        return value;
    }
}
