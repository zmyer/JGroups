package org.jgroups.util;

import java.util.Arrays;

/**
 * Simple string implemented as a byte[] array. Each character's higher 8 bits are truncated and
 * only the lower 8 bits are stored. AsciiString is mutable for efficiency reasons, but the chars
 * array should never be changed !
 *
 * @author Bela Ban
 * @since 3.5
 */
// TODO: 17/7/6 by zmyer
public class AsciiString implements Comparable<AsciiString> {
    protected final byte[] val;

    public AsciiString() {
        val = new byte[] {};
    }

    public AsciiString(String str) {
        int length = str != null ? str.length() : 0;
        this.val = new byte[length];
        for (int i = 0; i < length; i++)
            val[i] = (byte) str.charAt(i);
    }

    public AsciiString(AsciiString str) {
        this.val = str.val;
    }

    public AsciiString(byte[] val) {
        this.val = val; // mutable, used only for creation
    }

    public AsciiString(int length) {
        this.val = new byte[length];
    }

    public byte[] chars() {
        return val;
    } // mutable

    public int length() {
        return val.length;
    }

    public int compareTo(AsciiString str) {
        if (Arrays.hashCode(chars()) == Arrays.hashCode(str.val))
            return 0;

        int len1 = val.length;
        int len2 = str.val.length;
        int lim = Math.min(len1, len2);

        int k = 0;
        while (k < lim) {
            byte c1 = val[k];
            byte c2 = str.val[k];
            if (c1 != c2)
                return c1 > c2 ? 1 : -1;
            k++;
        }
        return len1 > len2 ? 1 : len1 < len2 ? -1 : 0;
    }

    public boolean equals(Object obj) {
        return obj instanceof AsciiString && equals(((AsciiString) obj).val);
    }

    public boolean equals(byte[] other) {
        return Arrays.equals(val, other);
    }

    public int hashCode() {
        int h = 0;
        if (val != null)
            for (byte aVal : val)
                h = 31 * h + aVal;
        return h;
    }

    public String toString() {
        return new String(val);
    }
}
