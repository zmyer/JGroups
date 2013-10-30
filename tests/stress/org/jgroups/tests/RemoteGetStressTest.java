package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.lang.reflect.Method;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests https://issues.jboss.org/browse/JGRP-1675
 * @author Bela Ban
 * @since  3.5
 */
public class RemoteGetStressTest {
    protected JChannel[]                  channels;
    protected Address[]                   target_members; // B, C, D
    protected RpcDispatcher[]             dispatchers;
    protected static final int            NUM_THREADS=500;
    protected static final int            SIZE=100 * 1000; // size of a GET response
    protected static final byte[]         BUF=new byte[SIZE];
    protected static final MethodCall     GET_METHOD;
    protected static final long           TIMEOUT=30000; // ms
    protected static final RequestOptions OPTIONS=RequestOptions.SYNC().setTimeout(TIMEOUT).setFlags(Message.Flag.OOB);

    static {
        try {
            Method get_method=RemoteGetStressTest.class.getMethod("get");
            GET_METHOD=new MethodCall(get_method);
        }
        catch(NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    protected void start() throws Exception {
        String[] names={"A", "B", "C", "D"};
        channels=new JChannel[4];
        dispatchers=new RpcDispatcher[channels.length];
        for(int i=0; i < channels.length; i++) {
            channels[i]=createChannel(names[i]);
            dispatchers[i]=new RpcDispatcher(channels[i], null, null, this);
            channels[i].connect("cluster");
        }
        Util.waitUntilAllChannelsHaveSameSize(10000, 500, channels);
        System.out.println("view A: " + channels[0].getView());

        target_members=new Address[]{channels[1].getAddress(), channels[2].getAddress(), channels[3].getAddress()};
        final AtomicInteger success=new AtomicInteger(0), failure=new AtomicInteger(0);

        insertDISCARD(channels[0], 0.2);

        Invoker[] invokers=new Invoker[NUM_THREADS];
        for(int i=0; i < invokers.length; i++) {
            invokers[i]=new Invoker(dispatchers[0], success, failure);
            invokers[i].start();
        }

        for(Invoker invoker: invokers)
            invoker.join();

        System.out.println("\n\n**** success: " + success + ", failure=" + failure);

        stop();
    }

    protected void stop() {
        for(RpcDispatcher disp: dispatchers)
            disp.stop();
        Util.close(channels);
    }

    protected static JChannel createChannel(String name) throws Exception {
        Protocol[] protocols={
          new SHARED_LOOPBACK().setValue("oob_thread_pool_min_threads", 1)
            .setValue("oob_thread_pool_max_threads", 5)
          .setValue("oob_thread_pool_queue_enabled", false),
          new PING().timeout(1000),
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          new GMS(),
          new UFC(),
          new MFC().setValue("max_credits", 2000000).setValue("min_threshold", 0.4),
          new FRAG2().fragSize(8000),
        };
        return new JChannel(protocols).name(name);
    }

    public static byte[] get() {
        Util.sleepRandom(5, 15);
        return BUF;
    }


    public static void main(String[] args) throws Exception {
        RemoteGetStressTest test=new RemoteGetStressTest();
        test.start();
    }

    protected Address randomMember() {
        return Util.pickRandomElement(target_members);
    }

    protected static void insertDISCARD(JChannel ch, double discard_rate) throws Exception {
        TP transport=ch.getProtocolStack().getTransport();
        DISCARD discard=new DISCARD();
        discard.setUpDiscardRate(discard_rate);
        ch.getProtocolStack().insertProtocol(discard, ProtocolStack.ABOVE, transport.getClass());
    }

    protected class Invoker extends Thread {
        protected final RpcDispatcher disp;
        protected final AtomicInteger success, failure;

        public Invoker(RpcDispatcher disp, AtomicInteger success, AtomicInteger failure) {
            this.disp=disp;
            this.success=success;
            this.failure=failure;
        }

        public void run() {
            Address target=randomMember();
            if(target == null) throw new IllegalArgumentException("target cannot be null");

            try {
                Future<byte[]> future=disp.callRemoteMethodWithFuture(target, GET_METHOD, OPTIONS);
                byte[] result=future.get(TIMEOUT,TimeUnit.MILLISECONDS);
                if(result != null) {
                    System.out.println("received " + result.length + " bytes");
                    success.incrementAndGet();
                }
                else {
                    failure.incrementAndGet();
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

}
