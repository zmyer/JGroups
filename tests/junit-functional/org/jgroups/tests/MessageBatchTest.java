package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.protocols.*;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Tests {@link org.jgroups.util.MessageBatch}
 * @author Bela Ban
 * @since  3.3
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class MessageBatchTest {
    protected static final short UNICAST=1, DISCOVERY=2, FD=3, MERGE=4, TP_ID=5;
    protected final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B");



    public void testCopyConstructor() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        System.out.println("batch = " + batch);
        assert batch.size() == msgs.size() : "batch: " + batch;
        batch.remove(3).remove(6).remove(10);
        System.out.println("batch = " + batch);
        assert batch.size() == msgs.size() -3 : "batch: " + batch;
    }

    public void testCapacityConstructor() {
        MessageBatch batch=new MessageBatch(3);
        assert batch.size() == 0;
    }

    public void testGet() {
        List<Message> msgs=createMessages();
        Message msg=msgs.get(5);
        MessageBatch batch=new MessageBatch(msgs);
        assert batch.get(5) == msg;

        for(int index: Arrays.asList(-1, msgs.size(), msgs.size() +1))
            assert batch.get(index) == null;
    }


    public void testSet() {
        List<Message> msgs=createMessages();
        Message msg=msgs.get(5);
        MessageBatch batch=new MessageBatch(msgs);
        assert batch.get(5) == msg;
        batch.set(4, msg);
        assert batch.get(4) == msg;
    }

    public void testAdd() {
        MessageBatch batch=new MessageBatch(3);
        List<Message> msgs=createMessages();
        for(Message msg: msgs)
            batch.add(msg);
        System.out.println("batch = " + batch);
        assert batch.size() == msgs.size() : "batch: " + batch;
    }

    public void testGetMatchingMessages() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        Collection<Message> matching=batch.getMatchingMessages(TP_ID, false);
        assert matching.size() == batch.size();
        assert batch.size() == msgs.size();

        matching=batch.getMatchingMessages(FD, true);
        assert matching.size() == 1;
        assert batch.size() == msgs.size() -1;

        int size=batch.size();
        matching=batch.getMatchingMessages(TP_ID, true);
        assert matching.size() == size;
        assert batch.size() == 0;
    }


    public void testTotalSize() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        long total_size=0;
        for(Message msg: msgs)
            total_size+=msg.size();
        System.out.println("total size=" + batch.totalSize());
        assert batch.totalSize() == total_size;
    }


    public void testSize() throws Exception {
        List<Message> msgs=createMessages();
        ByteArrayOutputStream output=new ByteArrayOutputStream();
        DataOutputStream out=new DataOutputStream(output);
        TP.writeMessageList(b, a, msgs, out, false);
        out.flush();

        byte[] buf=output.toByteArray();
        System.out.println("size=" + buf.length + " bytes, " + msgs.size() + " messages");

        DataInputStream in=new DataInputStream(new ByteArrayInputStream(buf));
        List<Message> list=TP.readMessageList(in);
        assert msgs.size() == list.size();
    }


    public void testIterator() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        int index=0, count=0;
        for(Message msg: batch) {
            Message tmp=msgs.get(index++);
            count++;
            assert msg == tmp;
        }
        assert count == msgs.size();
    }


    public void testIterator2() {
        List<Message> msgs=createMessages();
        MessageBatch batch=new MessageBatch(msgs);
        int count=0;
        for(Message msg: batch)
            count++;
        assert count == msgs.size();

        batch.remove(3).remove(5).remove(10);
        count=0;
        for(Message msg: batch)
            if(msg != null)
                count++;
        assert count == msgs.size() - 3;
    }


    protected List<Message> createMessages() {
        List<Message> retval=new ArrayList<Message>(10);

        for(long seqno=1; seqno <= 5; seqno++)
            retval.add(new Message(b).putHeader(UNICAST, UNICAST2.Unicast2Header.createDataHeader(seqno, (short)22, false)));

        retval.add(new Message(b).putHeader(DISCOVERY, new PingHeader(PingHeader.GET_MBRS_RSP, "demo-cluster")));
        retval.add(new Message(b).putHeader(FD, new FD.FdHeader(org.jgroups.protocols.FD.FdHeader.HEARTBEAT)));
        retval.add(new Message(b).putHeader(MERGE, MERGE3.MergeHeader.createViewResponse(Util.createView(a, 22, a,b))));

        for(long seqno=6; seqno <= 10; seqno++)
            retval.add(new Message(b).putHeader(UNICAST, UNICAST2.Unicast2Header.createDataHeader(seqno, (short)22, false)));

        for(Message msg: retval)
            msg.putHeader(TP_ID, new TpHeader("demo-cluster"));

        return retval;
    }
}
