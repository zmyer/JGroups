package org.jgroups.util;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Bela Ban
 * @since  4.0
 */
@SuppressWarnings("serial")
public class PaddedAtomicBoolean extends AtomicBoolean {
    protected volatile int i1,i2,i3,i4,i5,i6=1;
       protected volatile int i7,i8,i9,i10,i11,i12,i13,i14=2;

    public PaddedAtomicBoolean(boolean initialValue) {
        super(initialValue);
    }

    public int sum() {
        return i1+i2+i3+i4+i5+i6+i7+i8+i9+i10+i11+i12+i13+i14;
    }

}
