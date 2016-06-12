package org.jgroups.protocols;

import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Bela Ban
 * @since  3.6.10
 */
public class EncryptHeader extends Header implements Streamable {
    public static final byte ENCRYPT        = 1 << 0;
    public static final byte SECRET_KEY_REQ = 1 << 1;
    public static final byte SECRET_KEY_RSP = 1 << 2;

    protected byte   type;
    protected byte[] version;
    protected byte[] signature; // the encrypted checksum


    public EncryptHeader() {}


    public EncryptHeader(byte type, byte[] version) {
        this.type=type;
        this.version=version;
    }

    public byte          getType()           {return type;}
    public byte[]        getVersion()        {return version;}
    public byte[]        getSignature()         {return signature;}
    public void setSignature(byte[] s) {this.signature=s;}

    public void writeExternal(java.io.ObjectOutput out) throws IOException
    {
        out.writeByte(type);
        out.writeObject(version);
        out.writeObject(signature);
    }


    public void readExternal(java.io.ObjectInput in) throws IOException, ClassNotFoundException
    {
        type = in.readByte();
        version = (byte [])in.readObject();
        signature = (byte [])in.readObject();
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeByte(type);
        Util.writeByteBuffer(version, out);
        Util.writeByteBuffer(signature, out);
    }

    public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
        type=in.readByte();
        version=Util.readByteBuffer(in);
        signature=Util.readByteBuffer(in);
    }

    public String toString() {
        return String.format("[%s version=%s]", typeToString(type), (version != null? version.length + " bytes" : "n/a"));
    }

    public int size() {return Global.BYTE_SIZE + Util.size(version) + Util.size(signature) /*+ Util.size(payload) */;}

    protected static String typeToString(byte type) {
        switch(type) {
            case ENCRYPT:        return "ENCRYPT";
            case SECRET_KEY_REQ: return "SECRET_KEY_REQ";
            case SECRET_KEY_RSP: return "SECRET_KEY_RSP";
            default:             return "<unrecognized type " + type;
        }
    }
}
