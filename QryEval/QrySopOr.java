/**
 *  Copyright (c) 2023, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;

/**
 *  The OR operator for all retrieval models.
 */
public class QrySopOr extends QrySop {

  public  double getDefaultScore (RetrievalModel r, long docid)throws IOException {
    return 0.0;  
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
        (r.getClass().getName() + " doesn't support the OR operator.");
    }
  }
  
  private double getScoreIndri (RetrievalModel r) throws IOException {
    if (this.args.size() == 0) {
      return 0.0; 
    }
    Double score = null;
    int docid = this.docIteratorGetMatch();
    for (int i=0; i<this.args.size(); i++) {
      //  Java knows that the i'th query argument is a Qry object, but
      //  it does not know what type.  We know that OR operators can
      //  only have QrySop objects as children.  Cast the i'th query
      //  argument to QrySop so that we can call its getScore method.

      QrySop q_i = (QrySop) this.args.get(i);

      //  If the i'th query argument matches this document, update the
      //  score.
      double q_score;
      if (q_i.docIteratorHasMatch (r) &&
         (q_i.docIteratorGetMatch () == docid)) {
        q_score = 1.0 - q_i.getScore(r);
      } else {
        q_score = 1.0 - q_i.getDefaultScore(r, docid);
      }
      // System.out.println("qScore: " + q_score);
      
      if (score == null) {
        score = q_score;    
      } else {
        score *= q_score;
      }
      // System.out.println("AND score now: " + q_score);
    }
    return 1.0 - score;
  }

  private double getScoreBM25 (RetrievalModel r) throws IOException {
    double score = 0.0;
    if (this.args.size() == 0) {
      return 0.0; 
    }
    int docid = this.docIteratorGetMatch();
    for (int i=0; i<this.args.size(); i++) {

      //  Java knows that the i'th query argument is a Qry object, but
      //  it does not know what type.  We know that OR operators can
      //  only have QrySop objects as children.  Cast the i'th query
      //  argument to QrySop so that we can call its getScore method.

      QrySop q_i = (QrySop) this.args.get(i);

      if (q_i.docIteratorHasMatch (r) &&
         (q_i.docIteratorGetMatch () == docid)) {
        double q_score = q_i.getScore(r);
        score = Math.max (score, q_score);
      }
    }
    return score;
  }

  private double getScoreRankedBoolean (RetrievalModel r) throws IOException {
    double score = 0.0;
    if (this.args.size() == 0) {
      return 0.0; 
    }
    int docid = this.docIteratorGetMatch();
    for (int i=0; i<this.args.size(); i++) {

      //  Java knows that the i'th query argument is a Qry object, but
      //  it does not know what type.  We know that OR operators can
      //  only have QrySop objects as children.  Cast the i'th query
      //  argument to QrySop so that we can call its getScore method.

      QrySop q_i = (QrySop) this.args.get(i);

      //  If the i'th query argument matches this document, update the
      //  score.
      /* 
      if (q_i.getScore(r) != 0.0) {
        return 1.0;
      }
      */
      if (q_i.docIteratorHasMatch (r) &&
         (q_i.docIteratorGetMatch () == docid)) {
        double q_score = q_i.getScore(r);
        score = Math.max (score, q_score);
      }
    }
    /* 
    for (int i=0; i<this.args.size(); i++) {

      //  Java knows that the i'th query argument is a Qry object, but
      //  it does not know what type.  We know that OR operators can
      //  only have QrySop objects as children.  Cast the i'th query
      //  argument to QrySop so that we can call its getScore method.

      QrySop q_i = (QrySop) this.args.get(i);

      //  If the i'th query argument matches this document, update the
      //  score.
      double q_score = q_i.getScore(r);
      score = Math.max (score, q_score);
    }
    // System.out.println("Score from or: " + score);
    */
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

    // System.out.println("Current docid: " + docid + "external: " + Idx.getExternalDocid(docid));
    int docid = this.docIteratorGetMatch();
    for (int i=0; i<this.args.size(); i++) {

      //  Java knows that the i'th query argument is a Qry object, but
      //  it does not know what type.  We know that OR operators can
      //  only have QrySop objects as children.  Cast the i'th query
      //  argument to QrySop so that we can call its getScore method.

      QrySop q_i = (QrySop) this.args.get(i);

      //  If the i'th query argument matches this document, update the
      //  score.
      /* 
      if (q_i.getScore(r) != 0.0) {
        return 1.0;
      }
      */
      if (q_i.docIteratorHasMatch (r) &&
         (q_i.docIteratorGetMatch () == docid)) {
        return 1.0;
      }
    }

    return 0.0;
  }

}
