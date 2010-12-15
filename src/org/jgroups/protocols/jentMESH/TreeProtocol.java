package org.jgroups.protocols.jentMESH;

import org.jgroups.annotations.Property;
import org.jgroups.stack.Protocol;

public class TreeProtocol extends Protocol {
  /* ----------------------------------------   Shared Properties   -------------------------------------------- */
  @Property(description="How many connections should each peer try to maintain")
  protected short peerConMax = 3;
}
