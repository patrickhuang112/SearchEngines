/**
 *  Copyright (c) 2023, Carnegie Mellon University.  All Rights Reserved.
 */
import java.io.*;
import java.util.*;

/**
 *  This class implements the document score list data structure
 *  and provides methods for accessing and manipulating them.
 */
public class ExpansionTermList {

  //  A utility class to create a <internalDocid, externalDocid, score>
  //  object.

  private class ExpansionTermListEntry {
    private double score;
    private String term;

    private ExpansionTermListEntry(String term, double score) {
      this.term = term;
      this.score = score;
    }
  }

  /**
   *  A list of document ids and scores. 
   */
  private List<ExpansionTermListEntry> scores = new ArrayList<ExpansionTermListEntry>();

  /**
   *  Append a document score to a score list.
   *  @param docid An internal document id.
   *  @param score The document's score.
   */
  public void add(String term, double score) {
    scores.add(new ExpansionTermListEntry(term, score));
  }

  /**
   *  Get the score of the n'th entry.
   *  @param n The index of the requested document score.
   *  @return The document's score.
   */
  public double getScore(int n) {
    return this.scores.get(n).score;
  }

  public String getTerm(int n) {
    return this.scores.get(n).term;
  }


  /**
   *  Get the size of the score list.
   *  @return The size of the posting list.
   */
  public int size() {
    return this.scores.size();
  }

  /*
   *  Compare two ScoreListEntry objects.  Sort by score, then
   *  internal docid.
   *
   *  STUDENTS:: You may need to modify this or create a new
   *  comparator to sort ScoreLists appropriately for your HW.
   */
  public class ExpansionTermListComparator implements Comparator<ExpansionTermListEntry> {

    @Override
    public int compare(ExpansionTermListEntry s1, ExpansionTermListEntry s2) {
      if (s1.score > s2.score)
        return -1;
      else if (s1.score < s2.score)
        return 1;
      return s1.term.compareTo(s2.term) ;
    }
  }

  /**
   *  Sort the list by score and external document id.
   */
  public void sort () {
    Collections.sort(this.scores, new ExpansionTermListComparator());
  }

  public void invert() {
    Collections.reverse(this.scores); 
  }
  
  /**
   * Reduce the score list to the first num results to save on RAM.
   * 
   * @param num Number of results to keep.
   */
  public void truncate(int num) {
    List<ExpansionTermListEntry> truncated = new ArrayList<ExpansionTermListEntry>(this.scores.subList(0,
        Math.min(num, scores.size())));
    this.scores.clear();
    this.scores = truncated;
  }
}
