
package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * Encapsulates information about a cluster node, e.g. local address, coordinator's addresss, logical name and
 * physical address(es)
 * @author Bela Ban
 * @version $Id: PingData.java,v 1.1.2.1 2009/02/18 07:46:50 belaban Exp $
 */
public class PingData implements Serializable, Streamable {

    private static final long serialVersionUID=3634334590904551586L;

    private Address own_addr=null;
    private Address coord_addr=null;
    private boolean is_server=false;


    public PingData() {
    }

    public PingData(Address own_addr,Address coord_addr,boolean is_server) {
        this.own_addr=own_addr;
        this.coord_addr=coord_addr;
        this.is_server=is_server;
    }

    public boolean equals(Object obj) {
        if(!(obj instanceof PingData))
            return false;
        PingData other=(PingData)obj;
        return own_addr != null && own_addr.equals(other.own_addr);
    }

    public int hashCode() {
        int retval=0;
        if(own_addr != null)
            retval+=own_addr.hashCode();
        if(retval == 0)
            retval=super.hashCode();
        return retval;
    }

    public boolean isCoord() {
        return is_server && own_addr != null && coord_addr != null && own_addr.equals(coord_addr);
    }
    
    public boolean hasCoord(){
        return is_server && own_addr != null && coord_addr != null;
    }

    public int size() {
        int retval=Global.BYTE_SIZE * 3; // for is_server, plus 2 presence bytes
        if(own_addr != null) {
            retval+=Global.BYTE_SIZE; // 1 boolean for: IpAddress or other address ?
            retval+=own_addr.size();
        }
        if(coord_addr != null) {
            retval+=Global.BYTE_SIZE; // 1 boolean for: IpAddress or other address ?
            retval+=coord_addr.size();
        }
        return retval;
    }

    public Address getAddress() {
        return own_addr;
    }

    public Address getCoordAddress() {
        return coord_addr;
    }

    public boolean isServer() {
        return is_server;
    }

    public String toString() {
        return new StringBuilder("[own_addr=").append(own_addr)
                                              .append(", coord_addr=")
                                              .append(coord_addr)
                                              .append(", is_server=")
                                              .append(is_server)
                                              .append(", is_coord=" + isCoord())
                                              .append(']')
                                              .toString();
    }

    public void writeTo(DataOutputStream outstream) throws IOException {
        Util.writeAddress(own_addr, outstream);
        Util.writeAddress(coord_addr, outstream);
        outstream.writeBoolean(is_server);
    }

    public void readFrom(DataInputStream instream) throws IOException, IllegalAccessException, InstantiationException {
        own_addr=Util.readAddress(instream);
        coord_addr=Util.readAddress(instream);
        is_server=instream.readBoolean();
    }
}
