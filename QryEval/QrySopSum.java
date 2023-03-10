/**
 *  Copyright (c) 2023, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;

/**
 *  The OR operator for all retrieval models.
 */
public class QrySopSum extends QrySop {


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
        (r.getClass().getName() + " doesn't support the SUM operator.");
    }
  }

  private double getScoreIndri (RetrievalModel r) throws IOException {
    double score = 0.0;
    if (this.args.size() == 0) {
      return 0.0; 
    } 
    for (int i=0; i<this.args.size(); i++) {
      QrySop q_i = (QrySop) this.args.get(i);
      score += q_i.getScore(r);
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
          score += (q_i.getScore(r) * (((rm.k_3 + 1.0) * 1.0) / (rm.k_3 + 1.0)));
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
      if (q_i.docIteratorHasMatch (r) &&
         (q_i.docIteratorGetMatch () == docid)) {
          score += (q_i.getScore(r));
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
          score += (q_i.getScore(r));
      }
    }
    return score;
  }

}
