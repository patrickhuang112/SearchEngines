/*
 *  Copyright (c) 2023, Carnegie Mellon University.  All Rights Reserved.
 *  Version 3.11.
 *  
 *  Compatible with Lucene 8.1.1.
 */
import java.io.*;
import java.util.*;
import java.nio.charset.*;

/**
 *  This software illustrates the architecture for the portion of a
 *  search engine that evaluates queries.  It is a guide for class
 *  homework assignments, so it emphasizes simplicity over efficiency.
 *  It implements an unranked Boolean retrieval model, however it is
 *  easily extended to other retrieval models.  For more information,
 *  see the ReadMe.txt file.
 */
public class QryEval {

  //  --------------- Constants and variables ---------------------

  private static final String USAGE =
    "Usage:  java QryEval paramFile\n\n";
  
  private static CharsetEncoder asciiEncoder = 
    Charset.forName("US-ASCII").newEncoder();

  public static boolean isAsciiString (String s) {
    return asciiEncoder.canEncode(s);
  }
  
  //  --------------- Methods ---------------------------------------

  /**
   *  @param args The only argument is the parameter file name.
   *  @throws Exception Error accessing the Lucene index.
   */
  public static void main(String[] args) throws Exception {

    //  This is a timer that you may find useful.  It is used here to
    //  time how long the entire program takes, but you can move it
    //  around to time specific parts of your code.
    
    Timer timer = new Timer();
    timer.start ();

    //  Check that a parameter file is included, and that the required
    //  parameters are present.  Just store the parameters.  They get
    //  processed later during initialization of different system
    //  components.

    if (args.length < 1) {
      throw new IllegalArgumentException (USAGE);
    }

    Map<String, String> parameters = readParameterFile (args[0]);

    //  Open the index and initialize the retrieval model.

    Idx.open (parameters.get ("indexPath"));
    RetrievalModel model = initializeRetrievalModel (parameters);

    //  Perform experiments.
    processQueryFile(parameters, model);

    //  Clean up.
    
    timer.stop ();
    System.out.println ("Time:  " + timer);
  }

  /**
   *  Allocate the retrieval model and initialize it using parameters
   *  from the parameter file.
   *  @return The initialized retrieval model
   *  @throws IOException Error accessing the Lucene index.
   */
  private static RetrievalModel initializeRetrievalModel (Map<String, String> parameters)
    throws IOException {

    RetrievalModel model = null;
    String modelString = parameters.get ("retrievalAlgorithm").toLowerCase();

    if (modelString.equals("unrankedboolean")) {
      model = new RetrievalModelUnrankedBoolean();

    } else if (modelString.equals("rankedboolean")) {

      model = new RetrievalModelRankedBoolean();

    } else if (modelString.equals("indri")) {
      model = new RetrievalModelIndri(Integer.parseInt(parameters.get("Indri:mu")), 
                                      Double.parseDouble(parameters.get("Indri:lambda")));
    } else if (modelString.equals("bm25")) {
      model = new RetrievalModelBM25(Double.parseDouble(parameters.get("BM25:k_1")),
                                     Double.parseDouble(parameters.get("BM25:b")),
                                     Double.parseDouble(parameters.get("BM25:k_3")));
    }

    else {
      throw new IllegalArgumentException
        ("Unknown retrieval model " + parameters.get("retrievalAlgorithm"));
    }
      
    return model;
  }

  private static RetrievalModel initializePrfRetrievalModel (Map<String, String> parameters)
    throws IOException {

    RetrievalModel model = null;
    String modelString = parameters.get ("prf").toLowerCase();
    assert(modelString != "false");
    assert(modelString == "indri" || modelString == "bm25");
    if (modelString.equals("indri")) {
      model = new RetrievalModelIndri(Integer.parseInt(parameters.get("prf:Indri:mu")), 
                                      0.0, 
                                      Double.parseDouble(parameters.get("prf:Indri:origWeight")));
      
    } else if (modelString.equals("bm25")) {
      model = new RetrievalModelBM25(Double.parseDouble(parameters.get("prf:BM25:k_1")),
                                     Double.parseDouble(parameters.get("prf:BM25:b")),
                                     Double.parseDouble(parameters.get("prf:BM25:k_3")));
    }

    else {
      throw new IllegalArgumentException
        ("Unknown retrieval model " + parameters.get("retrievalAlgorithm"));
    }
      
    return model;
  }

  /**
   * Print a message indicating the amount of memory used. The caller can
   * indicate whether garbage collection should be performed, which slows the
   * program but reduces memory usage.
   * 
   * @param gc 
   *          If true, run the garbage collector before reporting.
   */
  public static void printMemoryUsage(boolean gc) {

    Runtime runtime = Runtime.getRuntime();

    if (gc)
      runtime.gc();

    System.out.println("Memory used:  "
        + ((runtime.totalMemory() - runtime.freeMemory()) / (1024L * 1024L)) + " MB");
  }

  static String getOriginalQueryString(String qString, RetrievalModel model) {
    String defaultOp = model.defaultQrySopName (); 
    return defaultOp + "(" + qString + ")";
  }

  /**
   * Process one query.
   * @param qString A string that contains a query.
   * @param model The retrieval model determines how matching and scoring is done.
   * @return Search results
   * @throws IOException Error accessing the index
   */
  static ScoreList processQuery(String qString, String trecEvalOutputLength, RetrievalModel model)
    throws IOException {

    String defaultOp = model.defaultQrySopName ();
    qString = defaultOp + "(" + qString + ")";
    // System.out.println("QSTRING: " + qString);
    Qry q = QryParser.getQuery (qString);

    // Show the query that is evaluated
    
    System.out.println("    --> " + q);
    
    if (q != null) {

      ScoreList results = new ScoreList ();
      
      if (q.args.size () > 0) {		// Ignore empty queries

        q.initialize (model);

        while (q.docIteratorHasMatch (model)) {
          int docid = q.docIteratorGetMatch ();
          // System.out.println("Current doc, Internal: " + docid + ", External: "+ Idx.getExternalDocid(docid));
          double score = ((QrySop) q).getScore (model);
          results.add (docid, score);
          q.docIteratorAdvancePast (docid);
        }
      }
      
      results.sort(); 
      results.truncate(Integer.parseInt(trecEvalOutputLength));
      return results;
    } else
      return null;
  }

  static String getLearnedQuery(ExpansionTermList topTerms) {
    StringBuilder query = new StringBuilder();
    query.append("#WAND ("); 
    for (int i = topTerms.size() - 1; i >= 0; --i) {
      String term = topTerms.getTerm(i); 
      double score = topTerms.getScore(i);
      query.append(score);
      query.append(" ");
      query.append(term);
      query.append(" ");
    }
    query.append(")"); 
    return query.toString();
  }

  static String getExpandedQuery(String originalQuery, String learnedQuery, double weight) {
    StringBuilder query = new StringBuilder();
    query.append("#WAND ("); 
    query.append(weight);
    query.append(" ");
    query.append(originalQuery);
    query.append(" ");
    query.append(1.0 - weight);
    query.append(" ");
    query.append(learnedQuery);
    query.append(" )"); 
    return query.toString();
  }


  static ExpansionTermList getExpansionTermIndri(ScoreList topDocs,
                                                 Map<String, String> parameters) {
    String field = "";
    if (parameters.containsKey("prf:expansionField")) {
      field = parameters.get("prf:expansionField");
    } else {
      field = "body"; 
    }

    ExpansionTermList terms = new ExpansionTermList();
    Double mu = Double.parseDouble(parameters.get("prf:Indri:mu"));
    HashMap<String, Double> seenTerms = new HashMap<String, Double>();
    Double sumOfPrevDocs = 0.0;
    double fieldlen = 0.0;
    try {
      fieldlen = ((double)Idx.getSumOfFieldLengths(field));
    } catch (IOException ex) {
      ex.printStackTrace();
      return terms;
    }

    int limit = Math.min(topDocs.size(), Integer.parseInt(parameters.get("prf:numDocs")));

    for (int i = 0; i < limit; ++i) {
      
      int docId = topDocs.getDocid(i); 
      double score = topDocs.getDocidScore(i);
      // System.out.println("DOCID: " + docId);
      // System.out.println("SCORE: " + score);
      try {
        TermVector tv = new TermVector(docId, field);
        double doclen = (double)tv.positionsLength();

        HashSet<String> unseenTerms = new HashSet<>(seenTerms.keySet());
        if (doclen == 0.0 && mu == 0.0) {
          continue; 
        }
        for (int j = 0; j < tv.stemsLength(); j++) {
          String term = tv.stemString(j); 
          if (term == null || term.contains(".") || term.contains(",") || !isAsciiString(term)) {
            continue; 
          }
          unseenTerms.remove(term);

          double termTf = (double)tv.stemFreq(j);
          double ctf = (double)(tv.totalStemFreq(j));
          double idf = Math.log(fieldlen / ctf);
          double ptc = ctf / fieldlen;
          

          double prevScore = 0.0;
          if (seenTerms.containsKey(term)) {
            prevScore = seenTerms.get(term); 
          } else {
            prevScore = ptc * sumOfPrevDocs * idf;
          }

          double ptd = (termTf + mu * ptc) / (doclen + mu);
          double thisScore = score * idf * ptd;
          double newScore = prevScore + thisScore; 
          seenTerms.put(term, newScore);
        }

        for (String term : unseenTerms) {
          double ctf = (double)(Idx.getTotalTermFreq(field, term));
          double idf = Math.log(fieldlen / ctf);
          double ptc = ctf / fieldlen;

          // We know termtf is 0 here
          double prevScore = seenTerms.get(term);
          double thisScore = score * ((mu * ptc) / (doclen + mu)) * idf;
          double newScore = prevScore + thisScore;
          seenTerms.put(term, newScore);
        }

        sumOfPrevDocs += ((mu * score) / (doclen + mu));
      } catch (IOException ex) {
        ex.printStackTrace(); 
      }
    }

    for (Map.Entry<String, Double> entry : seenTerms.entrySet()) {
      terms.add(entry.getKey(), entry.getValue());
    }

    terms.sort();
    terms.truncate(Integer.parseInt(parameters.get("prf:numTerms")));
    return terms;
  }


  /**
   *  Process the query file.
   *  @param queryFilePath Path to the query file
   *  @param model A retrieval model that will guide matching and scoring
   *  @throws IOException Error accessing the Lucene index.
   */
  static void processQueryFile(Map<String, String> parameters,
                               RetrievalModel model)
      throws IOException {

    String queryFilePath = parameters.get("queryFilePath"); 
    String trecEvalOutputPath = parameters.get("trecEvalOutputPath");
    String trecEvalOutputLength = parameters.get("trecEvalOutputLength");


    BufferedReader input = null;
    
    ArrayList<ScoreList> scoreLists = new ArrayList<>();
    ArrayList<String> queryIds = new ArrayList<>();
    ScoreList currentScoreList = new ScoreList();
    String currentQueryId = null;

    if (parameters.containsKey("prf:initialRankingFile")) {
      String initialRankingFilePath = parameters.get("prf:initialRankingFile");
      BufferedReader initialRanking = null;

      String rankingLine = null;
      try {
        initialRanking = new BufferedReader(new FileReader(initialRankingFilePath)); 
        while ((rankingLine = initialRanking.readLine()) != null) {
          String[] pair = rankingLine.split (" "); 
          // Should be 6 elements
          String queryId = pair[0]; 
          int docId = Idx.getInternalDocid(pair[2]);
          double score = Double.parseDouble(pair[4]);
          if (currentQueryId == null) {
            currentQueryId = queryId;
            queryIds.add(queryId);
          } else if (!currentQueryId.equals(queryId)) {
            scoreLists.add(currentScoreList);
            currentScoreList = new ScoreList();
            currentQueryId = queryId;
            queryIds.add(queryId);
          }
          currentScoreList.add(docId, score); 
        }

        if (currentScoreList.size() > 0) {
          scoreLists.add(currentScoreList); 
        }


      } catch (Exception ex) {
        ex.printStackTrace();
      } finally {
        initialRanking.close();
      }
    }

    try {
      String qLine = null;

      input = new BufferedReader(new FileReader(queryFilePath));

      //  Each pass of the loop processes one query.
      int queryIndex = 0;
      while ((qLine = input.readLine()) != null) {
        printMemoryUsage(false);
        ScoreList results = null;
        String qid;
        String[] pair = qLine.split(":");
        if (pair.length != 2) {
                throw new IllegalArgumentException
                  ("Syntax error:  Each line must contain one ':'.");
        }
        String query = pair[1];
        
        if (parameters.containsKey("prf:initialRankingFile")) {
          qid = queryIds.get(queryIndex); 
          results = scoreLists.get(queryIndex);
          results.truncate(Integer.parseInt(parameters.get("prf:numDocs")));
          
        } else {
          qid = pair[0];
          results = processQuery(query, trecEvalOutputLength, model);
        }
        
        if (results != null && (!parameters.containsKey("prf") || 
                                 parameters.get("prf") == "false")) {
          printResults(qid, results, trecEvalOutputPath);
        } else {
          ExpansionTermList prfQuery = getExpansionTermIndri(results, parameters);
          
          String originalQuery = getOriginalQueryString(query, model); 
          String learnedQuery = getLearnedQuery(prfQuery);
          String expandedQuery = getExpandedQuery(originalQuery, learnedQuery,
                                                  Double.parseDouble(parameters.get("prf:Indri:origWeight")));

          if (parameters.containsKey("prf:expansionQueryFile")) {
            System.out.println("LERANRED: " + learnedQuery);
            System.out.println("EXPAND: " + expandedQuery);
            printExpandedQuery(qid, learnedQuery, parameters.get("prf:expansionQueryFile"));
          }
          ScoreList expandedResults = processQuery(expandedQuery, trecEvalOutputLength, model); 
          printResults(qid, expandedResults, trecEvalOutputPath); 
        }

        queryIndex++;
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
      input.close();
    }
  }

  /**
   * Print the query results.
   * 
   * STUDENTS:: 
   * This is not the correct output format. You must change this method so
   * that it outputs in the format specified in the homework page, which is:
   * 
   * QueryID Q0 DocID Rank Score RunID
   * 
   * @param queryName
   *          Original query.
   * @param result
   *          A list of document ids and scores
   * @throws IOException Error accessing the Lucene index.
   */
  static void printResults(String queryName, 
                           ScoreList result,
                           String trecEvalOutputPath) throws IOException {

    try {
      FileWriter myWriter = new FileWriter(trecEvalOutputPath, true);
      if (result.size() == 0) {
        // System.out.println(queryName + " Q0 dummy 1 0 ?");
        myWriter.write(queryName + " Q0 dummyRecord 1 0 ?\n");
      } else {
        
        for (int i = 0; i < result.size(); i++) {
          int rank = i + 1;
          String toPrint = queryName + " Q0 " + Idx.getExternalDocid(result.getDocid(i)) + " " 
                              + Integer.toString(rank) + " " + result.getDocidScore(i) + " ?\n";
          // System.out.println(toPrint);
          myWriter.write(toPrint);
        }
      }
      myWriter.close();
      System.out.println("Successfully wrote to the file.");
    } catch (IOException e) {
      System.out.println("Output path invalid");
      e.printStackTrace();
    } 
  }

  static void printExpandedQuery(String originalQueryId, 
                                 String expandedQuery,
                                 String expansionQueryFilePath) throws IOException {
    try {
      FileWriter myWriter = new FileWriter(expansionQueryFilePath, true);
      myWriter.write(originalQueryId + ": " + expandedQuery + "\n");
      myWriter.close();
      System.out.println("Successfully wrote to the file.");
    } catch (IOException e) {
      System.out.println("Output path invalid");
      e.printStackTrace();
    } 
  }

  static void printExpansionList(String originalQueryId, 
                                 ExpansionTermList expandedQuery,
                                 String expansionQueryFilePath) throws IOException {
    try {
      FileWriter myWriter = new FileWriter(expansionQueryFilePath, true);
      if (expandedQuery.size() == 0) {
        // System.out.println(queryName + " Q0 dummy 1 0 ?");
        System.out.println("NO EXPANSION TERMS? ");
        assert(false);
      } else {
        System.out.println("YES EXPANSION TERMS ");
        for (int i = 0; i < expandedQuery.size(); i++) {
          String toPrint = "Score: " + expandedQuery.getScore(i) + ", term: " + expandedQuery.getTerm(i) + "\n";
          // System.out.println(toPrint);
          myWriter.write(toPrint);
        }
      }
      myWriter.close();
      System.out.println("Successfully wrote to the file.");
    } catch (IOException e) {
      System.out.println("Output path invalid");
      e.printStackTrace();
    } 
  }

  /**
   *  Read the specified parameter file, and confirm that the required
   *  parameters are present.  The parameters are returned in a
   *  HashMap.  The caller (or its minions) are responsible for processing
   *  them.
   *  @return The parameters, in <key, value> format.
   */
  private static Map<String, String> readParameterFile (String parameterFileName)
    throws IOException {

    Map<String, String> parameters = new HashMap<String, String>();
    File parameterFile = new File (parameterFileName);

    if (! parameterFile.canRead ()) {
      throw new IllegalArgumentException
        ("Can't read " + parameterFileName);
    }

    //  Store (all) key/value parameters in a hashmap.

    Scanner scan = new Scanner(parameterFile);
    String line = null;
    do {
      line = scan.nextLine();
      String[] pair = line.split ("=");
      parameters.put(pair[0].trim(), pair[1].trim());
      // System.out.println("PAIR: " + pair[0].trim() + ", " + pair[1].trim());
    } while (scan.hasNext());

    scan.close();

    //  Confirm that some of the essential parameters are present.
    //  This list is not complete.  It is just intended to catch silly
    //  errors.

    if (! (parameters.containsKey ("indexPath") &&
           parameters.containsKey ("queryFilePath") &&
           parameters.containsKey ("trecEvalOutputPath") &&
           parameters.containsKey ("retrievalAlgorithm"))) {
      throw new IllegalArgumentException
        ("Required parameters were missing from the parameter file.");
    }

    return parameters;
  }

}
