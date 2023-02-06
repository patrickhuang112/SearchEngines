/**
 *  Copyright (c) 2023, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;
import java.lang.IllegalArgumentException;

/**
 *  The SCORE operator for all retrieval models.
 */
public class QrySopScore extends QrySop {

  /**
   *  Document-independent values that should be determined just once.
   *  Some retrieval models have these, some don't.
   */
  
  /**
   *  Indicates whether the query has a match.
   *  @param r The retrieval model that determines what is a match
   *  @return True if the query matches, otherwise false.
   */
  public boolean docIteratorHasMatch (RetrievalModel r) {
    return this.docIteratorHasMatchFirst (r);
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
    } 

    //  STUDENTS::
    //  Add support for other retrieval models here.

    else {
      throw new IllegalArgumentException
        (r.getClass().getName() + " doesn't support the SCORE operator.");
    }
  }
  
  private double getScoreRankedBoolean (RetrievalModel r) throws IOException {
    QryIop qop = ((QryIop) this.args.get (0));
    int tf =  qop.getTfOfDoc();
    // System.out.println("TF WTF (ranked): " + (double)tf);
    return (double)tf;
    /* 
    int count = 0;
    double score = 0.0;
    if (this.args.size() == 0) {
      return 0.0; 
    }
    QryIop qop = ((QryIop) this.args.get (0));
    boolean match = qop.docIteratorHasMatch(r);
    if (!match) {
      return 0.0;
    }
    this.invertedList.getTf (this.docIteratorIndex)
    int docid = qop.docIteratorGetMatch();
    if (docid != this.docIteratorGetMatch()) {
      return 0.0; 
    }
    return 1.0;
    return score;
    */
  }

  /**
   *  getScore for the Unranked retrieval model.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  public double getScoreUnrankedBoolean (RetrievalModel r) throws IOException {

    //  Unranked Boolean systems return 1 for all matches.
    //
    //  Other retrieval models must do more work.  To help students
    //  understand how to implement other retrieval models, this
    //  method does a little more work.  
    //
    //  Java knows that the (only) query argument is a Qry object, but
    //  it does not know what type.  We know that SCORE operators can
    //  only have a single QryIop object as its child.  Cast the query
    //  argument to QryIop so that we can access its inverted list.

    /* 
    int tf =  qop.getTfOfDoc();
    // System.out.println("TF WTF (unranked): " + tf);
    if (tf == 0) {
      return 0.0;
    }
    */
    return 1.0;

    /* 
    QryIop qop = ((QryIop) this.args.get (0));
    boolean match = qop.docIteratorHasMatch(r);
    if (!match) {
      return 0.0;
    }
    int docid = qop.docIteratorGetMatch();
    if (docid != this.docIteratorGetMatch()) {
      return 0.0; 
    }
    return 1.0;
    */
    // return ((QryIop) this.args.get (0)).docIteratorHasMatch(r) ? 1.0 : 0.0;
  }

  /**
   *  Initialize the query operator (and its arguments), including any
   *  internal iterators.  If the query operator is of type QryIop, it
   *  is fully evaluated, and the results are stored in an internal
   *  inverted list that may be accessed via the internal iterator.
   *  @param r A retrieval model that guides initialization
   *  @throws IOException Error accessing the Lucene index.
   */
  public void initialize (RetrievalModel r) throws IOException {

    Qry q = this.args.get (0);
    q.initialize (r);

    /*
     *  STUDENTS:: In HW2 during query initialization you may find it
     *  useful to have this SCORE node precompute and cache some
     *  values that it will use repeatedly when calculating document
     *  scores.  It won't change your results, but it will improve the
     *  speed of your software.
     */
  }

}
