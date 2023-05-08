import java.io.*;
import java.util.*;

public class FeatureVector {

  private Double[] featureVector = new Double[QryEval.LAST_FEATURE];

  public void setFeature(int featureNumber, Double featureScore) {
    featureVector[featureNumber - 1] = featureScore; 
  }

  public Double getFeature(int featureNumber) {
    return featureVector[featureNumber - 1]; 
  }

}
