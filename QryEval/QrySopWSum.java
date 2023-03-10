/**
 *  Copyright (c) 2023, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;
import java.util.*;

/**
 *  The OR operator for all retrieval models.
 */
public class QrySopWSum extends QrySopWeighted {

  public  double getDefaultScore (RetrievalModel r, long docid)throws IOException {
    if (this.args.size() == 0) {
      return 0.0; 
    }
    assert(this.args.size() == this.weights.size());
    double score = 0.0;
    for (int i=0; i<this.args.size(); i++) {
      //  Java knows that the i'th query argument is a Qry object, but
      //  it does not know what type.  We know that OR operators can
      //  only have QrySop objects as children.  Cast the i'th query
      //  argument to QrySop so that we can call its getScore method.
      double weight = weights.get(i);
      QrySop q_i = (QrySop) this.args.get(i);

      //  If the i'th query argument matches this document, update the
      //  score.
      double q_score;
      if (q_i.docIteratorHasMatch (r) &&
         (q_i.docIteratorGetMatch () == docid)) {
        q_score = q_i.getScore(r);
      } else {
        q_score = q_i.getDefaultScore(r, docid);
      }
      score += (q_score * (weight / total_weight));
    }
    return score;
  }

  /**
   *  Indicates whether the query has a match.
   *  @param r The retrieval model that determines what is a match
   *  @return True if the query matches, otherwise false.
   */
  public boolean docIteratorHasMatch (RetrievalModel r) {
    return this.docIteratorHasMatchMin (r);
  }

  /**
   *  Get a score for the document that docIteratorHasMatch matched.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  public double getScore (RetrievalModel r) throws IOException {

    // Make sure we have enough weights for all args:
    if (this.args.size() != this.weights.size()) {
      throw new IllegalArgumentException
        (" Not enough weights for all args"); 
    } 

    if (r instanceof RetrievalModelUnrankedBoolean) {
      return this.getScoreUnrankedBoolean (r);
    } else if (r instanceof RetrievalModelRankedBoolean) {
      return this.getScoreRankedBoolean (r);
    } else if (r instanceof RetrievalModelIndri) {
      return this.getScoreIndri (r);
    } else if (r instanceof RetrievalModelBM25) {
      return this.getScoreBM25 (r);
    } else {
      throw new IllegalArgumentException
        (r.getClass().getName() + " doesn't support the WSUM operator.");
    }
  }

  private double getScoreIndri (RetrievalModel r) throws IOException {
    if (this.args.size() == 0) {
      return 0.0; 
    }
    assert(this.args.size() == this.weights.size());
    double score = 0.0;
    int docid = this.docIteratorGetMatch();
    for (int i=0; i<this.args.size(); i++) {
      //  Java knows that the i'th query argument is a Qry object, but
      //  it does not know what type.  We know that OR operators can
      //  only have QrySop objects as children.  Cast the i'th query
      //  argument to QrySop so that we can call its getScore method.
      double weight = weights.get(i);
      QrySop q_i = (QrySop) this.args.get(i);

      //  If the i'th query argument matches this document, update the
      //  score.
      double q_score;
      if (q_i.docIteratorHasMatch (r) &&
         (q_i.docIteratorGetMatch () == docid)) {
        q_score = q_i.getScore(r);
      } else {
        q_score = q_i.getDefaultScore(r, docid);
      }
      score += (q_score * (weight / total_weight));
    }
    return score;
  }

  private double getScoreBM25 (RetrievalModel r) throws IOException {
    double score = 0.0;
    if (this.args.size() == 0) {
      return 0.0; 
    } 
    RetrievalModelBM25 rm = (RetrievalModelBM25)r;
    int docid = this.docIteratorGetMatch();
    for (int i=0; i<this.args.size(); i++) {

      //  Java knows that the i'th query argument is a Qry object, but
      //  it does not know what type.  We know that OR operators can
      //  only have QrySop objects as children.  Cast the i'th query
      //  argument to QrySop so that we can call its getScore method.

      QrySop q_i = (QrySop) this.args.get(i);
      if (q_i.docIteratorHasMatch (r) &&
         (q_i.docIteratorGetMatch () == docid)) {
          score += (q_i.getScore(r) * (((rm.k_3 + 1.0) * weights.get(i)) / (rm.k_3 + weights.get(i))));
      }
    }
    return score;
  }
  
  private double getScoreRankedBoolean (RetrievalModel r) throws IOException {
    double score = 0.0;
    if (this.args.size() == 0) {
      return 0.0; 
    } 
    RetrievalModelBM25 rm = (RetrievalModelBM25)r;
    int docid = this.docIteratorGetMatch();
    for (int i=0; i<this.args.size(); i++) {

      //  Java knows that the i'th query argument is a Qry object, but
      //  it does not know what type.  We know that OR operators can
      //  only have QrySop objects as children.  Cast the i'th query
      //  argument to QrySop so that we can call its getScore method.

      QrySop q_i = (QrySop) this.args.get(i);
      if (q_i.docIteratorHasMatch (r) &&
         (q_i.docIteratorGetMatch () == docid)) {
          score += (q_i.getScore(r) * (weights.get(i)));
      }
    }
    return score;
  }
  /**
   *  getScore for the UnrankedBoolean retrieval model.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  private double getScoreUnrankedBoolean (RetrievalModel r) throws IOException {
    //  Unranked Boolean systems only have two scores:
    //  1 (document matches) and 0 (document doesn't match).  QryEval
    //  only calls getScore for documents that match, so if we get
    //  here, the document matches, and its score should be 1.  The
    //  most efficient implementation returns 1 from here.
    //
    //  Other retrieval models must do more work.  To help students
    //  understand how to implement other retrieval models, this
    //  method uses a more general solution.  OR takes the maximum
    //  of the scores from its children query nodes.

    double score = 0.0;
    if (this.args.size() == 0) {
      return 0.0; 
    } 
    RetrievalModelBM25 rm = (RetrievalModelBM25)r;
    int docid = this.docIteratorGetMatch();
    for (int i=0; i<this.args.size(); i++) {

      //  Java knows that the i'th query argument is a Qry object, but
      //  it does not know what type.  We know that OR operators can
      //  only have QrySop objects as children.  Cast the i'th query
      //  argument to QrySop so that we can call its getScore method.

      QrySop q_i = (QrySop) this.args.get(i);
      if (q_i.docIteratorHasMatch (r) &&
         (q_i.docIteratorGetMatch () == docid)) {
          score += (q_i.getScore(r) * (weights.get(i)));
      }
    }
    return score;
  }

}
