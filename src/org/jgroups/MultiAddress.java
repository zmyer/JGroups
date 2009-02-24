package org.jgroups;

import org.jgroups.util.Util;

import java.util.List;
import java.util.Arrays;
import java.io.*;

/**
 * Address which is a logical <em>multicast</em> address, but lists the destination members individually. When a
 * message with dest=MultiAddress is seen by the transport, it is processed as follows:
 * <ul>
 * <li>msg.dest is nulled
 * <li>for each address L of the address list of MultiAddress, the physical address P for L is looked up and
 * the message is sent to P. Note that msg.dest == null 
 * </ul>
 * @author Bela Ban
 * @version $Id: MultiAddress.java,v 1.1.2.3 2009/02/24 12:23:22 belaban Exp $
 */
public class MultiAddress implements Address {
    private static final long serialVersionUID=7187294882574443042L;
    private Address[]     addresses;
    private String        name;

    
    public MultiAddress() {
    }

    /**
     * Creates an instance
     * @param addresses A list of addresses. The list passed to the constructor should either be a copy of an
     * existing list, should be immutable, or should be a copy-on-write list, so that iterating over the list
     * doesn't throw a ConcurrentModificationException.
     * @param name Logical name, e.g. buddy pool or cluster name. If null, we'll use the list to compare
     */
    public MultiAddress(List<Address> addresses, String name) {
        if(addresses != null) {
            this.addresses=new Address[addresses.size()];
            int index=0;
            for(Address addr: addresses) {
                this.addresses[index++]=addr;
            }
        }
        this.name=name;
    }


    public boolean isMulticastAddress() {
        return true;
    }

    public Address[] getAddresses() {
        return addresses;
    }

    public String getName() {
        return name;
    }


    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(addresses);
        out.writeObject(name);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        addresses=(Address[])in.readObject();
        name=(String)in.readObject();
    }

    public void writeTo(DataOutputStream out) throws IOException {
        short length=(short)(addresses != null? addresses.length : 0);
        out.writeShort(length);
        if(length > 0) {
            for(Address addr: addresses)
                Util.writeAddress(addr, out);
        }
        Util.writeString(name, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        short length=in.readShort();
        if(length > 0) {
            for(short i=0; i < length; i++) {
                addresses[i]=Util.readAddress(in);
            }
        }
        name=Util.readString(in);
    }

    public int size() {
        int retval=Global.SHORT_SIZE  // length of addresses
                + Global.BYTE_SIZE;   // presence byte for string
        if(addresses != null) {
            for(Address addr: addresses)
                retval+=Util.size(addr);
        }

        if(name != null)
            retval+=name.length() + 2;

        return retval;
    }

    public int hashCode() {
        return name.hashCode();
    }

    public boolean equals(Object obj) {
        if(!(obj instanceof MultiAddress))
            throw new IllegalArgumentException(obj + " is not of type MultiAddress");
        MultiAddress other=(MultiAddress)obj;
        return name.equals(other.name);
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(Arrays.toString(addresses));
        if(name != null)
        sb.append(" (" + name + ")");
        return sb.toString();
    }

    public int compareTo(Address o) {
        MultiAddress other=(MultiAddress)o;
        return name.compareTo(other.name);
    }
}
