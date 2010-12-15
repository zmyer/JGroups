package org.jgroups.protocols.jentMESH.dataTypes;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.jgroups.Address;
import org.jgroups.util.Util;

public class HandshakeData implements Externalizable {
  private ConnectionType conType;
  private long attemptIdentifier;
  private boolean isResponse;
  private NodeConnectionDetails sourceConDetails;
  private Map<Address, NodeConnectionDetails> otherMeshNodes;
  private boolean dataSet;
  
  public HandshakeData() {
    // used for Externalizable only!
    dataSet = false;
    attemptIdentifier = -1;
  }
  
  public HandshakeData(ConnectionType conType, 
                       long attemptIdentifier, 
                       boolean isResponse, 
                       short peerConnections, 
                       short leafConnections, 
                       Map<Address, NodeConnectionDetails> knownNodes) {
    this.conType = conType;
    this.attemptIdentifier = attemptIdentifier;
    this.isResponse = isResponse;
    this.sourceConDetails = new NodeConnectionDetails(peerConnections, leafConnections);
    if (knownNodes != null) {
      otherMeshNodes = Collections.unmodifiableMap(knownNodes);
    } else {
      if (conType == ConnectionType.SplitPeer) {
        throw new RuntimeException("Can not create handshake data for a peer without giving known mesh state");
      }
      otherMeshNodes = null;
    }
    dataSet = true;
  }
  
  public ConnectionType getConType() {
    if (! dataSet) {
      throw new RuntimeException("Data has not been set in this object");
    }
    return conType;
  }
  
  public long getAttemptIdentifier() {
    return attemptIdentifier;
  }
  
  public boolean isResponse() {
    return isResponse;
  }
  
  public NodeConnectionDetails getSourceConDetails() {
    if (! dataSet) {
      throw new RuntimeException("Data has not been set in this object");
    }
    return sourceConDetails;
  }
  
  public Map<Address, NodeConnectionDetails> getOtherMeshNodes() {
    if (! dataSet) {
      throw new RuntimeException("Data has not been set in this object");
    }
    return otherMeshNodes;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    conType = ConnectionType.values()[in.readShort()];
    attemptIdentifier = in.readLong();
    isResponse = in.readBoolean();
    sourceConDetails = new NodeConnectionDetails();
    sourceConDetails.readExternal(in);
    otherMeshNodes = (Map<Address, NodeConnectionDetails>)in.readObject();
    dataSet = true;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeShort((short)conType.ordinal());
    out.writeLong(attemptIdentifier);
    out.writeBoolean(isResponse);
    sourceConDetails.writeExternal(out);
    out.writeObject(otherMeshNodes);
  }
}
