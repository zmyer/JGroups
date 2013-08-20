package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.protocols.pbcast.FLUSH;
import org.jgroups.protocols.pbcast.FLUSH.FlushHeader;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
        private final Address address;
        private final FLUSH flush;

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
                if(hdr != null &&  hdr.getType() == FlushHeader.STOP_FLUSH)
                    this.flushParticipants = hdr.getFlushParticipants();

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
