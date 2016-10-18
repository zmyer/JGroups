package org.jgroups.protocols;

/**
 * @author Bela Ban
 * @since x.y
 */

import org.jctools.queues.MpscArrayQueue;
import org.jgroups.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * This bundler adds all (unicast or multicast) messages to a queue until max size has been exceeded, but does send
 * messages immediately when no other messages are available. https://issues.jboss.org/browse/JGRP-1540
 */
public class JCTransferQueueBundler extends BaseBundler implements Runnable {
    protected MpscArrayQueue<Message> queue;
    protected List<Message>          remove_queue;
    protected volatile     Thread    bundler_thread;
    protected volatile boolean       running=true;
    protected static final String    THREAD_NAME="JCTransferQueueBundler";

    public JCTransferQueueBundler() {
        this.remove_queue=new ArrayList<>(16384);
    }

    protected JCTransferQueueBundler(MpscArrayQueue<Message> queue) {
        this.queue=queue;
        this.remove_queue=new ArrayList<>(16384);
    }

    public JCTransferQueueBundler(int capacity) {
        this(new MpscArrayQueue<Message>(capacity));
    }

    public Thread               getThread()               {return bundler_thread;}
    public int                  getBufferSize()           {return queue.size();}
    public int                  removeQueueSize()         {return remove_queue.size();}
    public JCTransferQueueBundler removeQueueSize(int size) {this.remove_queue=new ArrayList<>(size); return this;}

    public void init(TP transport) {
        super.init(transport);
        if(queue == null)
            queue=new MpscArrayQueue<>(16384);
    }

    public synchronized void start() {
        if(running)
            stop();
        bundler_thread=transport.getThreadFactory().newThread(this, THREAD_NAME);
        running=true;
        bundler_thread.start();
    }

    public synchronized void stop() {
        running=false;
        Thread tmp=bundler_thread;
        bundler_thread=null;
        if(tmp != null) {
            tmp.interrupt();
            if(tmp.isAlive()) {
                try {tmp.join(500);} catch(InterruptedException e) {}
            }
        }
        queue.clear();
    }


    public int size() {
        return super.size() + removeQueueSize() + getBufferSize();
    }

    public void send(Message msg) throws Exception {
        if(running) {
            queue.offer(msg);
            LockSupport.unpark(bundler_thread);
        }
    }

    public void run() {
        while(running) {
            Message msg=null;
            try {
                if(null == (msg=queue.poll()))
                    LockSupport.park();
                long size=msg.size();
                if(count + size >= transport.getMaxBundleSize())
                    _sendBundledMessages();
                _addMessage(msg, size);
                while(true) {
                    remove_queue.clear();
                    int num_msgs=queue.drain(el -> remove_queue.add(el), remove_queue.size());
                    if(num_msgs <= 0)
                        break;
                    for(int i=0; i < remove_queue.size(); i++) {
                        msg=remove_queue.get(i);
                        size=msg.size();
                        if(count + size >= transport.getMaxBundleSize())
                            _sendBundledMessages();
                        _addMessage(msg, size);
                    }
                }
                if(count > 0)
                    _sendBundledMessages();
            }
            catch(Throwable t) {
            }
        }
    }

    // This should not affect perf, as the lock is uncontended most of the time
    protected void _sendBundledMessages() {
        lock.lock();
        try {
            sendBundledMessages();
        }
        finally {
            lock.unlock();
        }
    }

    protected void _addMessage(Message msg, long size) {
        lock.lock();
        try {
            addMessage(msg, size);
        }
        finally {
            lock.unlock();
        }
    }

    protected static int assertPositive(int value, String message) {
        if(value <= 0) throw new IllegalArgumentException(message);
        return value;
    }
}
