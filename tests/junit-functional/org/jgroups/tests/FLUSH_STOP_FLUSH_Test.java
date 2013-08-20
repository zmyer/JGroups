package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.stack.*;
import org.jgroups.stack.Protocol;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.protocols.pbcast.FLUSH.FlushHeader;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Vector;

/**
 * Tests the FLUSH STOP_FLUSH behavior
 * @author Dennis Reed
 */
public class FLUSH_STOP_FLUSH_Test extends TestCase {
    IpAddress a1;
    FLUSH flush;
    StopFlushInterceptor downInterceptor;
    BlockInterceptor upInterceptor;

    public FLUSH_STOP_FLUSH_Test(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();

        a1=new IpAddress(1111);

        flush=new FLUSH();
        downInterceptor = new StopFlushInterceptor(a1, flush);
        downInterceptor.setUpProtocol(flush);
        flush.setDownProtocol(downInterceptor);

/*
        TP transport=new TP() {
            public boolean supportsMulticasting() {return false;}
            public void sendMulticast(byte[] data, int offset, int length) throws Exception {}
            public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {}
            public String getInfo() {return null;}
            public Object down(Event evt) { return null; }
            protected PhysicalAddress getPhysicalAddress() {return null;}
            public TimeScheduler getTimer() {return new DefaultTimeScheduler(1);}
        };
        downInterceptor.setDownProtocol(transport);
*/

        upInterceptor = new BlockInterceptor();
        flush.setUpProtocol(upInterceptor);

        flush.start();

        Vector<Address> members=new Vector<Address>(1);
        members.add(a1);
        View view=new View(a1, 1, members);

        // set the local address
        flush.up(new Event(Event.SET_LOCAL_ADDRESS,a1));

        // set dummy view
        flush.up(new Event(Event.VIEW_CHANGE,view));
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        flush.stop();
    }

    public void testStopFlush() throws InterruptedException {
        flush.down(new Event(Event.SUSPEND));
        assertTrue(upInterceptor.isBlocked());

        flush.down(new Event(Event.RESUME));
        assertFalse(upInterceptor.isBlocked());

        // Verify flushParticipants is set correctly on the STOP_FLUSH message
        Collection<Address> flushParticipants = downInterceptor.getFlushParticipants();
        assertNotNull(flushParticipants);
        assertEquals(1, flushParticipants.size());
        assertTrue(flushParticipants.contains(a1));
    }

    public void testRogueStopFlush() throws InterruptedException {
        flush.down(new Event(Event.SUSPEND));
        assertTrue(upInterceptor.isBlocked());

        // STOP_FLUSH that is not addressed to this member
        Address a2 = new IpAddress(2222);
        Message msg = new Message(null, a2, null);
        Collection<Address> flushMembers = new ArrayList<Address>();
        flushMembers.add(a2);
        msg.putHeader(flush.getName(), new FlushHeader(FlushHeader.STOP_FLUSH, 1, flushMembers));
        flush.up(new Event(Event.MSG, msg));

        // Should still be blocked
        assertTrue(upInterceptor.isBlocked());
    }

    static class StopFlushInterceptor extends Protocol {
        private Collection<Address> flushParticipants;
        private Address address;
        private FLUSH flush;
        private static Field typeField;
        private static Field flushParticipantsField;

        static {
            try {
                typeField = FlushHeader.class.getDeclaredField("type");
                typeField.setAccessible(true);

                flushParticipantsField = FlushHeader.class.getDeclaredField("flushParticipants");
                flushParticipantsField.setAccessible(true);
            }
            catch ( NoSuchFieldException e )
            {
                fail("FlushHeader is missing fields checked by test case");
            }
        }
        
        public StopFlushInterceptor ( Address address, FLUSH flush ) {
            this.address = address;
            this.flush = flush;
        }

        public String getName () {
            return "StopFlushInterceptor";
        }

        public Object down(Event evt) {
            if(evt.getType() == Event.MSG) {
                Message msg=(Message)evt.getArg();
                FlushHeader hdr=(FlushHeader)msg.getHeader(flush.getName());
                if(hdr != null) {
                    try {
                        byte type = typeField.getByte(hdr);
                        if(type == FlushHeader.STOP_FLUSH) {
                            this.flushParticipants = (Collection<Address>)flushParticipantsField.get(hdr);
                        }
                    }
                    catch ( IllegalAccessException e )
                    {
                        fail("Could not make FlushHeader fields used by test accessible");
                    }
                }

                // loopback
                if(msg.getDest() == null || msg.getDest().equals(this.address))
                {
                    msg.setSrc(this.address);
                    getUpProtocol().up(evt);
                }
            }

            return null;
        }

        public Collection<Address> getFlushParticipants ()
        {
            return this.flushParticipants;
        }
    }

    static class BlockInterceptor extends Protocol {
        private boolean blocked = false;
        
        public BlockInterceptor () {
        }

        public String getName () {
            return "BlockInterceptor";
        }

        public Object up(Event evt) {
            if(evt.getType() == Event.BLOCK) {
                this.blocked = true;
            } else if(evt.getType() == Event.UNBLOCK) {
                this.blocked = false;
            }

            return null;
        }

        public boolean isBlocked()
        {
            return this.blocked;
        }
    }
}
