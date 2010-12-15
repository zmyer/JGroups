package org.jgroups.protocols.jentMESH.dataTypes;

import org.jgroups.Address;

public class MeshIdentifier {
  private Address nodeAddress;
  private boolean addressSet = false;
  
  public MeshIdentifier() { }
  
  public MeshIdentifier(Address nodeAddress) {
    setAddress(nodeAddress);
  }
  
  public Address getAddress() {
    if (! addressSet) {
      throw new RuntimeException("Address has not been set");
    }
    return nodeAddress;
  }
  
  public boolean addressSet() {
    return addressSet;
  }
  
  public void setAddress(Address nodeAddress) {
    if (nodeAddress != null) {
      addressSet = true;
      this.nodeAddress = nodeAddress;
    }
  }
  
  public boolean equals(Object input) {
    if (input == null) {
      return false;
    } else if (this == input) {
      return true;
    } else if (input instanceof MeshIdentifier) {
      return this.equals((MeshIdentifier) input);
    } else {
      return false;
    }
  }
  
  public boolean equals(MeshIdentifier input) {
    return input != null &&
           (input.nodeAddress == null ? nodeAddress == null : input.nodeAddress.equals(nodeAddress)) &&
           super.equals(input);
  }
  
  public int hashCode() {
    return super.hashCode();
  }

  public String toString() {
    return (addressSet ? nodeAddress.toString() : "unknown");
  }
}
