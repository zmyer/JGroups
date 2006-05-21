// $Id: ClassMap.java,v 1.3.6.1 2006/05/21 09:34:56 mimbert Exp $

package org.jgroups.conf;


/**
 * Maintains mapping between magic number and class
 *
 * @author Filip Hanik (<a href="mailto:filip@filip.net">filip@filip.net)
 * @version 1.0
 */
public class ClassMap {
    private final String  mClassname;
    private final String  mDescription;
    private final boolean mPreload;
    private final int     mMagicNumber;

    public ClassMap(String clazz,
                    String desc,
                    boolean preload,
                    int magicnumber) {
        mClassname=clazz;
        mDescription=desc;
        mPreload=preload;
        mMagicNumber=magicnumber;
    }

    public int hashCode() {
        return getMagicNumber();
    }

    public String getClassName() {
        return mClassname;
    }

    public String getDescription() {
        return mDescription;
    }

    public boolean getPreload() {
        return mPreload;
    }

    public int getMagicNumber() {
        return mMagicNumber;
    }


    /**
     * Returns the Class object for this class<BR>
     */
    public Class getClassForMap() throws ClassNotFoundException {
        return Class.forName(getClassName(),false,Thread.currentThread().getContextClassLoader());
    }


    public boolean equals(Object another) {
        if(another instanceof ClassMap) {
            ClassMap obj=(ClassMap)another;
            return getClassName().equals(obj.getClassName());
        }
        else
            return false;
    }


}
