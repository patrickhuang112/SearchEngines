/**
 *  Copyright (c) 2023, Carnegie Mellon University.  All Rights Reserved.
 */
import java.io.*;
import java.util.*;

/**
 *  The SYN operator for all retrieval models.
 */
public class QryIopNear extends QryIop {

  

  private int distance;

  public QryIopNear(int distance) {
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

  private boolean alignAllLocationIterators (ArrayList<InvList.DocPosting> docPostings, 
                                             ArrayList<Integer> locationIterators) {
    // Assumes all are already at this docid;
    // System.out.println("Sizes: " + locationIterators.size() + ", " + docPostings.size());
    if (locationIterators.get(0) >= docPostings.get(0).positions.size())  {
      return false;
    }

    int left_location = docPostings.get(0).positions.get(locationIterators.get(0));
    for (int i = 1; i < this.args.size(); i++) {
      int locationIt = locationIterators.get(i);
      Vector<Integer> positions = docPostings.get(i).positions;
      while (true) {
        if (locationIt >= positions.size()) {
          break; 
        }
        int location = positions.get(locationIt);
        if (location > left_location) {
          left_location = location;
          break; 
        }
        locationIt++;
      }
      if (locationIt >= positions.size()) {
        return false; 
      }
      locationIterators.set(i, locationIt);
    }
    return true;
  }

  private void incrementAllLocationIterators (ArrayList<Integer> locationIterators) {
    for (int i = 0; i < locationIterators.size(); i++) {
      locationIterators.set(i, locationIterators.get(i) + 1); 
    }
  }

  // Returns -1 if not correct, otherwise the right most location iterator
  private int checkAllLocationIterators (ArrayList<InvList.DocPosting> docPostings,
                                          ArrayList<Integer> locationIterators) {
    int left_location = docPostings.get(0).positions.get(locationIterators.get(0));
    for (int i = 1; i < docPostings.size(); i++) {
      int cur_location = docPostings.get(i).positions.get(locationIterators.get(i));
      if (cur_location - left_location > distance) {
        return -1; 
      }
      left_location = cur_location;
    }
    // Final left_location is location of right most doc
    return left_location;
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
      // 396922 
      docPostings.clear();  
      ArrayList<Integer> locationIterators = new ArrayList<>(); 
      for (Qry q_i : this.args) {
        QryIop q_iop = (QryIop)q_i;
        InvList.DocPosting posting = q_iop.docIteratorGetMatchPosting();
        if (docid == 396922) {
          System.out.println("Posting docid: " + posting.docid + ", and size: " + posting.positions.size()); 
        }
        // System.out.println("Posting docid: " + posting.docid + ", and size: " + posting.positions.size());
        docPostings.add(posting);
        locationIterators.add(0);
      }

      ArrayList<Integer> matchingLocations = new ArrayList<>();  
      while (alignAllLocationIterators(docPostings, locationIterators)) {
        int rightmost_location = checkAllLocationIterators(docPostings, locationIterators);
        if (rightmost_location != -1) {
          incrementAllLocationIterators(locationIterators);
          matchingLocations.add(rightmost_location);
        } else {
          // Increment leftmost
          locationIterators.set(0, locationIterators.get(0) + 1); 
        }
      }
      if (!matchingLocations.isEmpty()) {
        // System.out.println("Docid: " + docid + ", extern: " + Idx.getExternalDocid(docid));
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
