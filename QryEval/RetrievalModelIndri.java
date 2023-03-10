/**
 *  Copyright (c) 2023, Carnegie Mellon University.  All Rights Reserved.
 */

/**
 *  An object that stores parameters for the unranked Boolean
 *  retrieval model (there are none) and indicates to the query
 *  operators how the query should be evaluated.
 */
public class RetrievalModelIndri extends RetrievalModel {
  public int mu; 
  public double lambda;

  public RetrievalModelIndri(int mu, double lambda) {
    System.out.println("Mu: " + mu + ", lambda: " + lambda);
    this.mu = mu; 
    this.lambda = lambda;
  }

  public String defaultQrySopName () {
    return new String ("#and");
  }

}
