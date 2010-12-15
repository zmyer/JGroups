package org.jgroups.protocols.jentMESH.dataTypes.headers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.jgroups.util.Util;

public class MeshHandshakeHeader extends TreeMeshHeader {
  public MeshHandshakeHeader() {
    super();
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public void readFrom(DataInputStream in) throws IOException, 
                                                  IllegalAccessException,
                                                  InstantiationException {
  }

  @Override
  public void writeTo(DataOutputStream out) throws IOException {
  }

  @Override
  public String toString() {
    return "MeshHandshakeHeader";
  }
}
