/** 
 *  Copyright (c) 2023, Carnegie Mellon University.  All Rights Reserved.
 */
import java.io.*;
import java.util.*;

/**
 *  The root class of all query operators that use a retrieval model
 *  to determine whether a query matches a document and to calculate a
 *  score for the document.  This class has two main purposes.  First, it
 *  allows query operators to easily recognize any nested query
 *  operator that returns a document scores (e.g., #AND (a #OR(b c)).
 *  Second, it is a place to store data structures and methods that are
 *  common to all query operators that calculate document scores.
 */
public abstract class QrySopWeighted extends QrySop {
  
  protected ArrayList<Double> weights = new ArrayList<>();
  double total_weight = 0.0;

  public void appendWeight(double w) {
    weights.add(w);
    total_weight += w;
    /* 
    System.out.println("Weights");
    for (Double d : weights) {
      System.out.println("," + d + ","); 
    }
    */
  }
}
