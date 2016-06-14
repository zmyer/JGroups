package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.protocols.RingBufferBundler;
import org.jgroups.protocols.TP;
import org.jgroups.util.AsciiString;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.RingBuffer;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Bela Ban
 * @since  4.0
 */
@Test(singleThreaded=true)
public class RingBundlerTest {
    protected static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B"),
      c=Util.createRandomAddress("C"), d=Util.createRandomAddress("D");

    public void testReceiveAndSend() throws Exception {
        RingBufferBundler bundler=new RingBufferBundler(16);
        RingBuffer<Message> rb=bundler.buf();
        MockTransport transport=new MockTransport();
        bundler.init(transport);

        for(int i =0; i < 6; i++)
            bundler.send(new Message(null));
        System.out.println("rb = " + rb);
        int cnt=rb.countLockLockless();
        assert cnt == 6;
        bundler.sendBundledMessages(rb.buf(), rb.readIndexLockless(), cnt, rb.capacity());
        rb.publishReadIndex(cnt);
        System.out.println("rb = " + rb);
        assert rb.readIndex() == 6;
        assert rb.writeIndex() == 6;
        assert rb.count() == 0;

        for(Message msg: create(10000, null, a,a,a,b,c,d,d,a, null, null, a))
            bundler.send(msg);
        System.out.println("rb = " + rb);
        cnt=rb.countLockLockless();
        assert cnt == 12;
        assert rb.readIndex() == 6;
        assert rb.writeIndex() == 2;

        bundler.sendBundledMessages(rb.buf(), rb.readIndexLockless(), rb.countLockLockless(), rb.capacity());
        rb.publishReadIndex(cnt);

        assert rb.readIndex() == 2;
        assert rb.writeIndex() == 2;
        assert rb.count() == 0;
    }


    protected List<Message> create(int msg_size, Address ... destinations) {
        List<Message> list=new ArrayList<>(destinations.length);
        for(Address dest: destinations)
            list.add(new Message(dest, new byte[msg_size]));
        return list;
    }


    protected static class MockTransport extends TP {

        public MockTransport() {
            this.cluster_name=new AsciiString("mock");
            global_thread_factory=new DefaultThreadFactory("", false);
        }

        public boolean supportsMulticasting() {
            return false;
        }

        public void doSend(byte[] buf, int offset, int length, Address dest) throws Exception {

        }

        public void sendMulticast(byte[] data, int offset, int length) throws Exception {

        }

        public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {

        }

        public String getInfo() {
            return null;
        }

        protected PhysicalAddress getPhysicalAddress() {
            return null;
        }
    }
}
