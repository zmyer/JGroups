package org.jgroups.protocols.jentMESH.dataTypes;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class NodeConnectionDetails implements Externalizable {
  private static final long serialVersionUID = 7855121216367318005L;
  private short peerConnections;
  private short leafConnections;
  
  public NodeConnectionDetails() {
    peerConnections = -1;
    leafConnections = -1;
  }
  
  public NodeConnectionDetails(short peerCon, short leafCon) {
    peerConnections = peerCon;
    leafConnections = leafCon;
  }
  
  public boolean isSet() {
    return peerConnections >= 0 && leafConnections >= 0;
  }
  
  public void setPeerConnections(short peerConnections) {
    this.peerConnections = peerConnections;
  }
  
  public void setLeafConnections(short leafConnections) {
    this.leafConnections = leafConnections;
  }
  
  public short getPeerConnections() {
    return peerConnections;
  }
  
  public short getLeafConnections() {
    return leafConnections;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, 
                                                  ClassNotFoundException {
    peerConnections = in.readShort();
    leafConnections = in.readShort();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeShort(peerConnections);
    out.writeShort(leafConnections);
  }
}
