package org.jgroups.protocols.jentMESH.dataTypes;

import java.io.Serializable;

public enum ConnectionType implements Serializable {
  Leaf, SplitPeer, ParentPeer, Unknown;
  public boolean isLeaf() {
    return this == Leaf;
  }
  
  public boolean isPeer() {
    return this == SplitPeer || this == ParentPeer;
  }
  
  public String toString() {
    if (this == Leaf) {
      return "Leaf";
    } else if (this == SplitPeer) {
      return "SplitPeer";
    }  else if (this == ParentPeer) {
      return "ParentPeer";
    } else {
      return "Unknown";
    }
  }
}
