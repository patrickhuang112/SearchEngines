/**
 *  Copyright (c) 2023, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;
import java.lang.IllegalArgumentException;

/**
 *  The SCORE operator for all retrieval models.
 */
public class QrySopScore extends QrySop {

  public double getDefaultScore (RetrievalModel r, long docid)throws IOException {
    // System.out.println("Getting default");
    RetrievalModelIndri rm = (RetrievalModelIndri)r; 
    QryIop qop = ((QryIop) this.args.get (0));
    double doc_len = (double)(Idx.getFieldLength(qop.getField(), (int)docid));
    double ctf_qop = (double)qop.getCtf();
    double ctf = (ctf_qop == 0.0) ? 0.5 : ctf_qop;
    double pqc = ctf / ((double)Idx.getSumOfFieldLengths(qop.getField()));
    // System.out.println("QTF " + qop.getCtf());
    // System.out.println("PQC " + pqc);
    double p1 = (1.0 - rm.lambda) * ((rm.mu * pqc) / (doc_len + rm.mu));
    double p2 = rm.lambda * pqc;
    return p1 + p2; 
  }

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
    } else if (r instanceof RetrievalModelIndri) {
      return this.getScoreIndri (r);
    } else if (r instanceof RetrievalModelBM25) {
      return this.getScoreBM25 (r);
    } else {
      throw new IllegalArgumentException
        (r.getClass().getName() + " doesn't support the SCORE operator.");
    }
  }

  private double getScoreIndri (RetrievalModel r) throws IOException {
    // System.out.println("Getting actual indri");
    RetrievalModelIndri rm = (RetrievalModelIndri)r; 
    QryIop qop = ((QryIop) this.args.get (0));
    double tf =  (double)(qop.getTfOfDoc());
    double doc_len = (double)(Idx.getFieldLength(qop.getField(), qop.docIteratorGetMatch()));
    double ctf_qop = (double)qop.getCtf();
    double ctf = (ctf_qop == 0.0) ? 0.5 : ctf_qop;
    double pqc = ctf / ((double)Idx.getSumOfFieldLengths(qop.getField()));
    double p1 = (1.0 - rm.lambda) * ((tf + (rm.mu * pqc)) / (doc_len + rm.mu));
    double p2 = rm.lambda * pqc;
    return p1 + p2;
  }

  private double getScoreBM25 (RetrievalModel r) throws IOException {
    RetrievalModelBM25 rm = (RetrievalModelBM25)r;
    QryIop qop = ((QryIop) this.args.get (0));
    double tf =  (double)(qop.getTfOfDoc());
    double df = (double)(qop.getDf());
    double num_docs_n = (double)Idx.getNumDocs();
    double num_docs_field = (double)(Idx.getDocCount(qop.getField()));
    double doc_len = (double)(Idx.getFieldLength(qop.getField(), qop.docIteratorGetMatch()));
    double avg_doc_len = ((double)(Idx.getSumOfFieldLengths(qop.getField()))) / num_docs_field;
    double p1 = Math.max(0.0, Math.log(((num_docs_n - df + 0.5) / (df + 0.5))));
    double p2 = tf / (tf + rm.k_1 * ((1.0 - rm.b) + rm.b * (doc_len / avg_doc_len)));
    return p1*p2;
  }
  
  private double getScoreRankedBoolean (RetrievalModel r) throws IOException {
    QryIop qop = ((QryIop) this.args.get (0));
    int tf =  qop.getTfOfDoc();
    return (double)tf;
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

    return 1.0;
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
