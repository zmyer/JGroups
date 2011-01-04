package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.Global;

import java.io.*;

/** Logical address which is unique over space and time.
 * <br/>
 * Copied from java.util.UUID, but unneeded fields from the latter have been removed. UUIDs needs to
 * have a small memory footprint.
 * @author Bela Ban
 */
public class IdAddress implements Address, Streamable, Comparable<Address> {
    private static final long serialVersionUID=-28777882492099481L;

    short id;




    public IdAddress() {
    }


    public IdAddress(short id) {
        this.id=id;
    }


    public String toString() {
        return String.valueOf(id);
    }


    /**
     * Returns a hash code for this {@code UUID}.
     * @return  A hash code value for this {@code UUID}
     */
    public int hashCode() {
        return id;
    }

    /**
     * Compares this object to the specified object.  The result is {@code
     * true} if and only if the argument is not {@code null}, is a {@code UUID}
     * object, has the same variant, and contains the same value, bit for bit,
     * as this {@code UUID}.
     * @param  obj The object to be compared
     * @return  {@code true} if the objects are the same; {@code false} otherwise
     */
    public boolean equals(Object obj) {
        if(!(obj instanceof IdAddress))
            return false;
        IdAddress tmp = (IdAddress)obj;
        return this == tmp || id == tmp.id;
    }


    /**
     * Compares this UUID with the specified UUID.
     * <p> The first of two UUIDs is greater than the second if the most
     * significant field in which the UUIDs differ is greater for the first UUID.
     * @param  other {@code UUID} to which this {@code UUID} is to be compared
     * @return  -1, 0 or 1 as this {@code UUID} is less than, equal to, or greater than {@code val}
     */
    public int compareTo(Address other) {
        IdAddress val=(IdAddress)other;
        if(this == val)
            return 0;
        return id < val.id? -1 : id > val.id? 1 : 0;
    }



    public void writeTo(DataOutputStream out) throws IOException {
        out.writeShort(id);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        id=in.readShort();
    }

    public boolean isMulticastAddress() {
        return false;
    }

    public int size() {
        return Global.SHORT_SIZE;
    }

    public Object clone() throws CloneNotSupportedException {
        return new IdAddress(id);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeShort(id);
    }


    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id=in.readShort();
    }


}
