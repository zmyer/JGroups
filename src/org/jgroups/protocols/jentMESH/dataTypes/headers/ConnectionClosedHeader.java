package org.jgroups.protocols.jentMESH.dataTypes.headers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ConnectionClosedHeader extends TreeMeshHeader {
  private long attemptIdentifier;
  
  public ConnectionClosedHeader() {
    super();
    this.attemptIdentifier = -1;
  }
  
  public ConnectionClosedHeader(long attemptIdentifier) {
    super();
    this.attemptIdentifier = attemptIdentifier;
  }
  
  public long getAttemptIdentifier() {
    return attemptIdentifier;
  }

  @Override
  public String toString() {
    if (attemptIdentifier >= 0) {
      return "ConnectionClosedHeader Attempt: " + attemptIdentifier;
    } else {
      return "ConnectionClosedHeader (stable)";
    }
  }

  @Override
  public int size() {
    return Long.SIZE / 8;
  }

  @Override
  public void readFrom(DataInputStream in) throws IOException, 
                                                  IllegalAccessException,
                                                  InstantiationException {
    this.attemptIdentifier = in.readLong();
  }

  @Override
  public void writeTo(DataOutputStream out) throws IOException {
    out.writeLong(this.attemptIdentifier);
  }
}
