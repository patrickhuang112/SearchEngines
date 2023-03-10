/**
 *  Copyright (c) 2023, Carnegie Mellon University.  All Rights Reserved.
 */
import java.io.*;
import java.util.*;

/**
 *  The SYN operator for all retrieval models.
 */
public class QryIopWindow extends QryIop {

  private int distance;

  public QryIopWindow(int distance) {
    this.distance = distance;
  }

  // Returns -1 if none found (all have been advanced)
  private int findMatchingDocId (ArrayList<Integer> docIterators) {
    ArrayList<Integer> smallests = new ArrayList<>();
    while (true) {
      QryIop first_iop = (QryIop)(this.args.get(0));
      if (!first_iop.docIteratorHasMatch(null)) {
        return -1;
      }
      boolean same = true;
      int smallest_docid = first_iop.docIteratorGetMatch();
      smallests.clear();      
      smallests.add(0);

      for (int i = 1; i < docIterators.size(); i++) { 
        QryIop q_iop = (QryIop)(this.args.get(i));
        if (!q_iop.docIteratorHasMatch(null)) {
          return -1;
        } 
        int docid = q_iop.docIteratorGetMatch(); 
        if (docid != smallest_docid) {
          same = false; 
          if (docid < smallest_docid) {
            smallest_docid = docid; 
            smallests.clear();
            smallests.add(i);
          }
        } else {
          smallests.add(i); 
        }
      } 
      if (same) {
        return smallest_docid;
      } 
      for (int i : smallests) {
        QryIop q_iop = (QryIop)(this.args.get(i)); 
        q_iop.docIteratorAdvancePast(smallest_docid);
      } 
    } 
  }

  private void incrementAllLocationIterators (ArrayList<Integer> locationIterators) {
    for (int i = 0; i < locationIterators.size(); i++) {
      locationIterators.set(i, locationIterators.get(i) + 1); 
    }
  }

  private void incrementMinLocationIterators (ArrayList<Integer> minIterators,
                                              ArrayList<Integer> locationIterators) {
    for (Integer i : minIterators) {
      locationIterators.set(i, locationIterators.get(i) + 1); 
    }
  }

  private ArrayList<Integer> findMinLocations(ArrayList<InvList.DocPosting> docPostings,
                              ArrayList<Integer> locationIterators) {
    int smallest = Integer.MAX_VALUE;
    ArrayList<Integer> smallest_indexes = new ArrayList<>();
    for (int i = 0; i < locationIterators.size(); i++) {
      int location = docPostings.get(i).positions.get(locationIterators.get(i));
      if (location < smallest) {
        smallest = location;
        smallest_indexes.clear();
        smallest_indexes.add(i);
      } else if (location == smallest) {
        smallest_indexes.add(i);
      } 
    }
    return smallest_indexes;
  }
  
  private ArrayList<Integer> findMaxLocations(ArrayList<InvList.DocPosting> docPostings,
                              ArrayList<Integer> locationIterators) {
    int largest = -1;
    ArrayList<Integer> largest_indexes = new ArrayList<>();
    for (int i = 0; i < locationIterators.size(); i++) {
      int location = docPostings.get(i).positions.get(locationIterators.get(i));
      if (location > largest) {
        largest = location;
        largest_indexes.clear();
        largest_indexes.add(i);
      } else if (location == largest) {
        largest_indexes.add(i);
      } 
    }
    return largest_indexes;
  }

  private boolean checkLocationIterators (ArrayList<InvList.DocPosting> docPostings, 
                                          ArrayList<Integer> locationIterators) {
    for (int i = 0; i < this.args.size(); i++) {
      if (locationIterators.get(i) >= docPostings.get(i).positions.size()) {
        return false;
      }
    }
    return true;
  }

  /**
   *  Evaluate the query operator; the result is an internal inverted
   *  list that may be accessed via the internal iterators.
   *  @throws IOException Error accessing the Lucene index.
   */
  protected void evaluate () throws IOException {
    this.invertedList = new InvList (this.getField());
    if (args.size () == 0) {
      return;
    }

    ArrayList<Integer> docIterators = new ArrayList<>();
    for (Qry q_i: this.args) {
      QryIop q_iop = (QryIop)q_i;
      // If any of the terms doesn't even have a match, we already have an
      // empty inv list
      if (!q_iop.docIteratorHasMatch(null)){
        return;
      }
      docIterators.add(q_iop.docIteratorGetMatch());
    }
    //  Each pass of the loop adds 1 document to result inverted list
    //  until all of the argument inverted lists are depleted.

    ArrayList<InvList.DocPosting> docPostings = new ArrayList<>(); 
    while (true) {
      int docid = findMatchingDocId(docIterators);
      if (docid == -1) {
        return; 
      }
      docPostings.clear();  
      ArrayList<Integer> locationIterators = new ArrayList<>(); 
      for (Qry q_i : this.args) {
        QryIop q_iop = (QryIop)q_i;
        InvList.DocPosting posting = q_iop.docIteratorGetMatchPosting();
        docPostings.add(posting);
        locationIterators.add(0);
      }

      ArrayList<Integer> matchingLocations = new ArrayList<>();  
      while (checkLocationIterators(docPostings, locationIterators)) {
        ArrayList<Integer> mins = findMinLocations(docPostings, locationIterators); 
        ArrayList<Integer> maxs = findMaxLocations(docPostings, locationIterators); 
        assert(!mins.isEmpty() && !maxs.isEmpty());
        int min_location = docPostings.get(mins.get(0)).positions.get(locationIterators.get(mins.get(0)));
        int max_location = docPostings.get(maxs.get(0)).positions.get(locationIterators.get(maxs.get(0)));
        if (max_location - min_location < distance) {
          matchingLocations.add(max_location);
          incrementAllLocationIterators(locationIterators);
        } else {
          incrementMinLocationIterators(mins, locationIterators);
        }
      }
      
      if (!matchingLocations.isEmpty()) {
        invertedList.appendPosting(docid, matchingLocations);
      }
      // Finish with this doc
      for (Qry q_i : this.args) {
        QryIop q_iop = (QryIop)q_i;
        q_iop.docIteratorAdvancePast(docid);
      }
    }
  }

}
