import java.io.*;
import java.util.*;

public class FeatureVectorFileLine {
  public int relevanceScore;
  public int internalDocid;
  public String externalDocid;
  public String queryId; 
  public FeatureVector fv;

  public FeatureVectorFileLine(int relevanceScore, int internalDocid, 
                               String externalDocid, String queryId, 
                               FeatureVector fv) {
    this.relevanceScore = relevanceScore;
    this.internalDocid = internalDocid;
    this.externalDocid = externalDocid;
    this.queryId = queryId; 
    this.fv = fv;
  }

  public String toString(boolean forSvm, Set<Integer> disabledFeatures) {
    StringBuilder toWrite = new StringBuilder();
    toWrite.append(relevanceScore); 
    toWrite.append(" qid:");
    toWrite.append(queryId);
    toWrite.append(" "); 
    // int toPrintFeature = 1;
    if (forSvm) {
      for (int i = 1; i < QryEval.LAST_FEATURE; ++i) {
        if (disabledFeatures.contains(i)) {
          // toPrintFeature++;
          continue; 
        }
        Double val = fv.getFeature(i);
        if (val != null) {
          toWrite.append(i); 
          toWrite.append(":");
          toWrite.append(val.toString()); 
          toWrite.append(" ");
        } 
        // toPrintFeature++; 
      }
    } else {
      for (int i = 1; i < QryEval.LAST_FEATURE; ++i) {
        if (disabledFeatures.contains(i)) {
          continue; 
        } 
        toWrite.append(i); 
        toWrite.append(":"); 
        Double val = fv.getFeature(i);
        if (val == null) {
          toWrite.append("0"); 
        } else {
          toWrite.append(val.toString()); 
        }
        toWrite.append(" ");
      }
    }
    toWrite.append("# "); 
    toWrite.append(externalDocid); 
    toWrite.append("\n"); 
    return toWrite.toString();
  }
}
