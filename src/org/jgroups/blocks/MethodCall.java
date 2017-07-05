package org.jgroups.blocks;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.jgroups.Constructable;
import org.jgroups.util.Bits;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

/**
 * A method call is the JGroups representation of a remote method.
 * It includes the name of the method (case sensitive) and a list of arguments.
 * A method call is serializable and can be passed over the wire.
 *
 * @author Bela Ban
 */
// TODO: 17/7/4 by zmyer
public class MethodCall implements Streamable, Constructable<MethodCall> {
    protected short mode;
    protected String method_name;
    private short method_id; // the ID of a method, maps to a java.lang.reflect.Method
    protected Object[] args;      // the arguments to the call
    protected Class[] types;     // the types of the arguments, e.g., new Class[]{String.class, int.class}
    protected Method method;

    protected static final short METHOD = 1;
    private static final short TYPES = 2; // use types of all args to determine the method to be called
    protected static final short ID = 3; // use an ID to map to a method

    /** Needed for deserialization */
    public MethodCall() {
    }

    public MethodCall(Method method, Object... arguments) {
        init(method);
        if (arguments != null)
            args = arguments;
    }

    public MethodCall(short method_id, Object... args) {
        this.method_id = method_id;
        this.mode = ID;
        this.args = args;
    }

    public MethodCall(String method_name, Object[] args, Class[] types) {
        this.method_name = method_name;
        this.args = args;
        this.types = types;
        this.mode = TYPES;
    }

    public Supplier<? extends MethodCall> create() {
        return MethodCall::new;
    }

    public int getMode() {
        return mode;
    }

    public int mode() {
        return mode;
    }

    public String getMethodName() {
        return method_name;
    }

    String methodName() {
        return method_name;
    }

    public MethodCall setMethodName(String n) {
        method_name = n;
        return this;
    }

    public MethodCall methodName(String n) {
        method_name = n;
        return this;
    }

    public short getMethodId() {
        return method_id;
    }

    short methodId() {
        return method_id;
    }

    public MethodCall setMethodId(short id) {
        this.method_id = id;
        return this;
    }

    public MethodCall methodId(short id) {
        this.method_id = id;
        return this;
    }

    public Object[] getArgs() {
        return args;
    }

    public Object[] args() {
        return args;
    }

    public MethodCall args(Object... args) {
        this.args = args;
        return this;
    }

    public MethodCall setArgs(Object... args) {
        this.args = args;
        return this;
    }

    public Method getMethod() {
        return method;
    }

    public Method method() {
        return method;
    }

    public MethodCall setMethod(Method m) {
        init(m);
        return this;
    }

    public MethodCall method(Method m) {
        init(m);
        return this;
    }

    /**
     * Invokes the method with the supplied arguments against the target object.
     *
     * @param target - the object that you want to invoke the method on
     * @return the result
     */
    public Object invoke(Object target) throws Exception {
        if (target == null)
            throw new IllegalArgumentException("target is null");

        Class cl = target.getClass();
        Method meth = null;

        switch (mode) {
            case METHOD:
                if (this.method != null)
                    meth = this.method;
                break;
            case TYPES:
                meth = getMethod(cl, method_name, types);
                break;
            case ID:
                break;
            default:
                throw new IllegalStateException("mode " + mode + " is invalid");
        }

        if (meth != null) {
            try {
                return meth.invoke(target, args);
            } catch (InvocationTargetException target_ex) {
                Throwable exception = target_ex.getTargetException();
                if (exception instanceof Error)
                    throw (Error) exception;
                else if (exception instanceof RuntimeException)
                    throw (RuntimeException) exception;
                else if (exception instanceof Exception)
                    throw (Exception) exception;
                else
                    throw new RuntimeException(exception);
            }
        } else
            throw new NoSuchMethodException(method_name);
    }

    public Object invoke(Object target, Object[] args) throws Exception {
        if (args != null)
            this.args = args;
        return invoke(target);
    }

    /**
     * Called by the ProbeHandler impl. All args are strings. Needs to find a method where all
     * parameter types are primitive types, so the strings can be converted
     */
    public static Method findMethod(Class target_class, String method_name,
        Object[] args) throws Exception {
        int len = args != null ? args.length : 0;
        Method retval = null;
        Method[] methods = getAllMethods(target_class);
        for (Method m : methods) {
            if (m.getName().equals(method_name)) {
                Class<?>[] parameter_types = m.getParameterTypes();
                if (parameter_types.length == len) {
                    retval = m;
                    // now check if all parameter types are primitive types:
                    boolean all_primitive = true;
                    for (Class<?> parameter_type : parameter_types) {
                        if (!isPrimitiveType(parameter_type)) {
                            all_primitive = false;
                            break;
                        }
                    }
                    if (all_primitive)
                        return m;
                }
            }
        }
        return retval;
    }

    public String toString() {
        StringBuilder ret = new StringBuilder();
        boolean first = true;
        if (method_name != null)
            ret.append(method_name);
        else
            ret.append(method_id);
        ret.append('(');
        if (args != null) {
            for (Object arg : args) {
                if (first)
                    first = false;
                else
                    ret.append(", ");
                ret.append(arg);
            }
        }
        ret.append(')');
        return ret.toString();
    }

    public String toStringDetails() {
        StringBuilder ret = new StringBuilder();
        ret.append("MethodCall ");
        if (method_name != null)
            ret.append("name=").append(method_name);
        else
            ret.append("id=").append(method_id);
        ret.append(", number of args=").append((args != null ? args.length : 0)).append(')');
        if (args != null) {
            ret.append("\nArgs:");
            for (Object arg : args) {
                ret.append("\n[").append(arg).append(" (").
                    append((arg != null ? arg.getClass().getName() : "null")).append(")]");
            }
        }
        return ret.toString();
    }

    public void writeTo(DataOutput out) throws Exception {
        writeTo(out, null);
    }

    public void writeTo(DataOutput out, Marshaller marshaller) throws Exception {
        out.write(mode);

        switch (mode) {
            case METHOD:
                Bits.writeString(method_name, out);
                writeMethod(out);
                break;
            case TYPES:
                Bits.writeString(method_name, out);
                writeTypes(out);
                break;
            case ID:
                out.writeShort(method_id);
                break;
            default:
                throw new IllegalStateException("mode " + mode + " unknown");
        }
        writeArgs(out, marshaller);
    }

    public void readFrom(DataInput in) throws Exception {
        readFrom(in, null);
    }

    public void readFrom(DataInput in, Marshaller marshaller) throws Exception {
        switch (mode = in.readByte()) {
            case METHOD:
                method_name = Bits.readString(in);
                readMethod(in);
                break;
            case TYPES:
                method_name = Bits.readString(in);
                readTypes(in);
                break;
            case ID:
                method_id = in.readShort();
                break;
            default:
                throw new IllegalStateException("mode " + mode + " unknown");
        }
        readArgs(in, marshaller);
    }

    protected void init(Method method) {
        this.method = method;
        this.mode = METHOD;
        method_name = method.getName();
    }

    /**
     * Returns the first method that matches the specified name and parameter types. The overriding
     * methods have priority. The method is chosen from all the methods of the current class and all
     * its superclasses and superinterfaces.
     *
     * @return the matching method or null if no matching method has been found.
     */
    @SuppressWarnings("unchecked")
    protected static Method getMethod(Class target, String methodName, Class[] types) {
        if (types == null)
            types = new Class[0];

        Method[] methods = getAllMethods(target);
        methods:
        for (Method m : methods) {
            if (!methodName.equals(m.getName()))
                continue;
            Class[] parameters = m.getParameterTypes();
            if (types.length != parameters.length) {
                continue;
            }
            for (int j = 0; j < types.length; j++) {
                if (!parameters[j].isAssignableFrom(types[j])) {
                    continue methods;
                }
            }
            return m;
        }
        return null;
    }

    private void writeArgs(DataOutput out, Marshaller marshaller) throws Exception {
        int args_len = args != null ? args.length : 0;
        out.write(args_len);
        if (args_len == 0)
            return;
        for (Object obj : args) {
            if (marshaller != null)
                marshaller.objectToStream(obj, out);
            else
                Util.objectToStream(obj, out);
        }
    }

    private void readArgs(DataInput in, Marshaller marshaller) throws Exception {
        int args_len = in.readByte();
        if (args_len == 0)
            return;
        args = new Object[args_len];
        for (int i = 0; i < args_len; i++)
            args[i] = marshaller != null ?
                marshaller.objectFromStream(in) : Util.objectFromStream(in);
    }

    private void writeTypes(DataOutput out) throws Exception {
        int types_len = types != null ? types.length : 0;
        out.write(types_len);
        if (types_len > 0)
            for (Class<?> type : types)
                Util.objectToStream(type, out);
    }

    private void readTypes(DataInput in) throws Exception {
        int types_len = in.readByte();
        if (types_len > 0) {
            types = new Class<?>[types_len];
            for (int i = 0; i < types_len; i++)
                types[i] = Util.objectFromStream(in);
        }
    }

    private void writeMethod(DataOutput out) throws Exception {
        if (method != null) {
            out.write(1);
            Util.objectToStream(method.getParameterTypes(), out);
            Util.objectToStream(method.getDeclaringClass(), out);
        } else
            out.write(0);
    }

    @SuppressWarnings("unchecked")
    private void readMethod(DataInput in) throws Exception {
        if (in.readByte() == 1) {
            Class[] parametertypes = Util.objectFromStream(in);
            Class declaringclass = Util.objectFromStream(in);
            try {
                method = declaringclass.getDeclaredMethod(method_name, parametertypes);
            } catch (NoSuchMethodException e) {
                throw new IOException(e.toString());
            }
        }
    }

    /**
     * The method walks up the class hierarchy and returns <i>all</i> methods of this class
     * and those inherited from superclasses and superinterfaces.
     */
    @SuppressWarnings("unchecked")
    private static Method[] getAllMethods(Class target) {
        Class superclass = target;
        List methods = new ArrayList();
        int size = 0;

        while (superclass != null) {
            try {
                Method[] m = superclass.getDeclaredMethods();
                methods.add(m);
                size += m.length;
                superclass = superclass.getSuperclass();
            } catch (SecurityException e) {
                // if it runs in an applet context, it won't be able to retrieve methods from superclasses that belong
                // to the java VM and it will raise a security exception, so we catch it here.
                superclass = null;
            }
        }

        Method[] result = new Method[size];
        int index = 0;
        for (Object method1 : methods) {
            Method[] m = (Method[]) method1;
            System.arraycopy(m, 0, result, index, m.length);
            index += m.length;
        }
        return result;
    }

    private static boolean isPrimitiveType(Class<?> type) {
        return type.isPrimitive()
            || type == String.class
            || type == Boolean.class
            || type == Character.class
            || type == Byte.class
            || type == Short.class
            || type == Integer.class
            || type == Long.class
            || type == Float.class
            || type == Double.class;
    }
}