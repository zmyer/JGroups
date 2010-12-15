package org.jgroups.protocols.jentMESH.dataTypes;

import java.io.Serializable;

import org.jgroups.Address;

public class PeerRecommendationData implements Serializable {
  private long attemptIdentifier;
  private Address recommendedNode;
  
  public PeerRecommendationData() {
    // should only be used by externalizable
    attemptIdentifier = -1;
    recommendedNode = null;
  }
  
  // this constructor should be used only when it is not in relation to a connection attempt
  public PeerRecommendationData(Address recommendedNode) {
    this(-1, recommendedNode);
  }
  
  public PeerRecommendationData(long attemptIdentifier, Address recommendedNode) {
    this.attemptIdentifier = attemptIdentifier;
    this.recommendedNode = recommendedNode;
  }
  
  public long getAttemptIdentifier() {
    return attemptIdentifier;
  }
  
  public Address getRecommendedNode() {
    return recommendedNode;
  }
}
