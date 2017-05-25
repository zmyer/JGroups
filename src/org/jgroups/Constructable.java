package org.jgroups;

import java.util.function.Supplier;

/**
 * Interface returning a supplier which can be called to create an instance
 *
 * @author Bela Ban
 * @since 4.0
 */

// TODO: 17/5/25 by zmyer
public interface Constructable<T> {
    /** Creates an instance of the class implementing this interface */
    Supplier<? extends T> create();
}
