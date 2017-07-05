package org.jgroups.blocks;

import java.lang.reflect.Method;

/**
 * @author Bela Ban
 */
// TODO: 17/7/4 by zmyer
public interface MethodLookup {
    Method findMethod(short id);
}
