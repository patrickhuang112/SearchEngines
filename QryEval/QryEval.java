/*
 *  Copyright (c) 2023, Carnegie Mellon University.  All Rights Reserved.
 *  Version 3.11.
 *  
 *  Compatible with Lucene 8.1.1.
 */
import java.io.*;
import java.util.*;
import java.nio.charset.*;
import ciir.umass.edu.eval.Evaluator;

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

  public static int LAST_FEATURE = 21;

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

    
    

    if (parameters.containsKey("retrievalAlgorithm") && 
        parameters.get("retrievalAlgorithm").equals("ltr")) {
      performLearningToRank(parameters);

    } else if (parameters.containsKey("diversity") && 
               parameters.get("diversity").toLowerCase().equals("true")) {
      RetrievalModel model = initializeRetrievalModel (parameters); 
      performDiversification(parameters, model);
    } else {
      //  Perform experiments.
      RetrievalModel model = initializeRetrievalModel (parameters);
      processQueryFile(parameters, model);      
    }
    

    //  Clean up.
    
    timer.stop ();
    System.out.println ("Time:  " + timer);
  }

  private static class QueryAndIntents {
    public String qid = "";
    public ScoreList queryScoreList = new ScoreList();
    public ArrayList<ScoreList> intentScoreLists = new ArrayList<>();
    public ArrayList<String> intents = new ArrayList<>();
    public Map<Integer, List<Double>> intentScores  = new HashMap<>();
    public Double largest = Double.MIN_VALUE;

    public void normalize(int maxInputDocs) {
      boolean needsNormalize = false;
      int limit = Integer.min(maxInputDocs, queryScoreList.size());

      // Truncate all first
      queryScoreList.sort();
      queryScoreList.truncate(limit);
      queryScoreList.sort();
      for (int i = 1; i < intentScoreLists.size(); ++i) {
        ScoreList isl = intentScoreLists.get(i);
        isl.sort();
        isl.truncate(limit); 
        isl.sort();
      }

      Set<Integer> queryRankingDocids = new HashSet<>(); 
      for (int i = 0 ; i < queryScoreList.size(); ++i) {
        int docid = queryScoreList.getDocid(i);
        queryRankingDocids.add(docid); 
        intentScores.put(docid, new ArrayList<Double>(Collections.nCopies(intentScoreLists.size(), 0.0)));
      }
      Double sum = 0.0;
      largest = Double.MIN_VALUE;
      for (int i = 1; i < intentScoreLists.size(); ++i) {
        ScoreList isl = intentScoreLists.get(i);
        sum = 0.0;
        int k = 0;
        for (int j = 0; j < isl.size(); ++j) {
          int docid = isl.getDocid(j);
          double docScore = 0.0; 
          if (queryRankingDocids.contains(docid)) {
            k++;
            docScore = isl.getDocidScore(j);
            if (docScore > 1.0) {
              needsNormalize = true; 
            }
            try {
              // System.out.println("DOCSCORE: " + docScore + ", for docid: " + Idx.getExternalDocid(docid) );
            } catch (Exception e) {

            }
            sum += docScore;
            intentScores.get(docid).set(i, docScore);
          } 
        }
        // System.out.println("SUM_intent_" + i + ": " + sum);
        largest = Double.max(largest, sum); 
      }
      sum = 0.0;
      for (int j = 0; j < queryScoreList.size(); ++j) {
        double score = queryScoreList.getDocidScore(j);
        if (score > 1.0) {
          needsNormalize = true; 
        } 
        sum += score;
      }
      // System.out.println("SUM_query: " + sum);
      largest = Double.max(largest, sum); 
      if (!needsNormalize) {
        largest = 1.0; 
      }

      /* 
      // Largest value found, normalize all scores now
      for (int i = 0 ; i < queryScoreList.size(); ++i) {
        double prevScore = queryScoreList.getDocidScore(i);
        queryScoreList.setDocidScore(i, prevScore / largest);
      }
      */
    }

    public QueryAndIntents (String qid, ScoreList qsl, ArrayList<ScoreList> isl) {
      this.qid = qid;
      queryScoreList = qsl; 
      intentScoreLists = isl;
    } 

    public QueryAndIntents (String qid, ArrayList<String> intents) {
      this.qid = qid;
      this.intents = intents;
    }

    public void setScoreLists(ScoreList qsl, ArrayList<ScoreList> isl) {
      queryScoreList = qsl; 
      intentScoreLists = isl;
    }

    public Double getQueryScore(int position) {
      return queryScoreList.getDocidScore(position) / largest;
    }

    public Double getIntentScore(int docid, int intentNumber) {
      return intentScores.get(docid).get(intentNumber) / largest;
    }

    public void print() {
      System.out.println("Largest: " + largest);
      System.out.println("Query Id: " + qid);
      System.out.println("Query scorelist: ");
      for (int j = 0; j < queryScoreList.size(); ++j) {
        int docid = queryScoreList.getDocid(j);
        double docscore = queryScoreList.getDocidScore(j);
        System.out.println("DocID: " + docid + ", score: " + docscore);
      }
      System.out.println("Intent score lists: ");
      for (int i = 1; i < intentScoreLists.size(); ++i) {
        ScoreList sl = intentScoreLists.get(i);
        System.out.println("Intent number: " + i);
        for (int j = 0; j < sl.size(); ++j) {
          int docid = sl.getDocid(j);
          double docscore = sl.getDocidScore(j);
          System.out.println("DocID: " + docid + ", score: " + docscore);
        }
      }
      System.out.println();
    }

  } 
  
  private static class IndPair {
    public int docid;
    public int index;

    IndPair(int docid, int index) {
      this.docid = docid; 
      this.index = index;
    }
  }

  private static void performDiversification(Map<String, String> parameters, RetrievalModel model) throws Exception {
    ArrayList<QueryAndIntents> qaiList = new ArrayList<>(); 
    ScoreList queryScoreList = null;
    ScoreList currentScoreList = new ScoreList();
    ArrayList<ScoreList> intentScoreLists = new ArrayList<>();
    String currentQueryId = null;
    int currentQueryIntent = 0;

    String trecEvalOutputPath = parameters.get("trecEvalOutputPath");

    double lambda = Double.parseDouble(parameters.get("diversity:lambda"));
    String searchInputDocSize = parameters.get("diversity:maxInputRankingsLength");
    int trecEvalOutputLen = Integer.parseInt(parameters.get("trecEvalOutputLength"));
    int maxInputDocs = Integer.parseInt(parameters.get("diversity:maxInputRankingsLength"));
    int maxResultDocs = Integer.parseInt(parameters.get("diversity:maxResultRankingLength"));

    if (parameters.containsKey("diversity:initialRankingFile")) {
      String initialRankingFilePath = parameters.get("diversity:initialRankingFile");
      BufferedReader initialRanking = null;

      String rankingLine = null;
      try {
        initialRanking = new BufferedReader(new FileReader(initialRankingFilePath)); 
        while ((rankingLine = initialRanking.readLine()) != null) {
          // System.out.println("RANKING LINE: " + rankingLine); 
          String[] pair = rankingLine.split (" "); 
          // Should be 6 elements
          String queryId = pair[0]; 
          int docId = Idx.getInternalDocid(pair[2]);
          double score = Double.parseDouble(pair[4]);
          if (currentQueryId == null) {
            currentQueryId = queryId;
            // System.out.println("First queryID: " + currentQueryId);
          } else if (!currentQueryId.equals(queryId)) {
            String[] intentPair = queryId.split ("\\.");

            if (intentPair.length == 2 && intentPair[0].equals(currentQueryId)) {
              if (queryScoreList == null) {
                // System.out.println("First Query Intent found");
                queryScoreList = currentScoreList; 
              }
              Integer intent = Integer.parseInt(intentPair[1]);
              if (currentQueryIntent != intent) {
                // System.out.println("Different, adding to intent score lists: " + currentScoreList.size());
                intentScoreLists.add(currentScoreList); 
                currentScoreList = new ScoreList();
              } 
              // System.out.println("Current query Intent: " + currentQueryIntent + ", Intent number: " + intent + ", num of intent score lists: " + intentScoreLists.size());
              currentQueryIntent = intent;
              
            } else {
              intentScoreLists.add(currentScoreList);
              currentQueryIntent = 0;
              qaiList.add(new QueryAndIntents(currentQueryId, queryScoreList, intentScoreLists)); 

              // System.out.println("New queryID: " + queryId + ", and split size: " + intentPair.length);

              queryScoreList = null;
              intentScoreLists = new ArrayList<>();
              currentScoreList = new ScoreList();
              currentQueryId = queryId;
            }
          }
          currentScoreList.add(docId, score); 
        }

        if (queryScoreList != null && currentScoreList.size() > 0) {
          intentScoreLists.add(currentScoreList);
          qaiList.add(new QueryAndIntents(currentQueryId, queryScoreList, intentScoreLists)); 
        }
      } catch (Exception ex) {
        ex.printStackTrace();
      } finally {
        initialRanking.close();
      }
    } else {
      String qfp = parameters.get("queryFilePath");
      String trecEvalOutputLength = parameters.get("trecEvalOutputLength");
      String intentsFilePath = parameters.get("diversity:intentsFile");
      BufferedReader input = null;
      try {
        input = new BufferedReader(new FileReader(intentsFilePath));
        String intentLine = null;

        // Pad by 1
        ArrayList<String> intentList = new ArrayList<>();
        intentList.add("");

        while ((intentLine = input.readLine()) != null) {
          String[] pair = intentLine.split(":"); 
          String intent = pair[1];
          String[] qi = (pair[0]).split("\\.");
          currentQueryIntent = Integer.parseInt(qi[1]);
          if (currentQueryId == null) {
            currentQueryId = qi[0];
          } 
          if (!currentQueryId.equals(qi[0])) {
            qaiList.add(new QueryAndIntents(currentQueryId, intentList));
            currentQueryId = qi[0];
            intentList = new ArrayList<>(); 
            intentList.add("");
          } 
          intentList.add(intent);
        }
        
        if (intentList.size() > 0) {
          qaiList.add(new QueryAndIntents(currentQueryId, intentList));
        }

        String qLine = null;
        input.close();
        input = new BufferedReader(new FileReader(qfp));
        
        //  Each pass of the loop processes one query.
        int queryIndex = 0;
        while ((qLine = input.readLine()) != null) {
          printMemoryUsage(false);
          String qid;
          String[] pair = qLine.split(":");
          if (pair.length != 2) {
            throw new IllegalArgumentException
              ("Syntax error:  Each line must contain one ':'.");
          }
          String query = pair[1];
          
          qid = pair[0];
          queryScoreList = processQuery(query, searchInputDocSize, model);
          QueryAndIntents qai = qaiList.get(queryIndex);
          assert(qai.qid == qid);
          intentScoreLists = new ArrayList<>();
          intentScoreLists.add(new ScoreList());
          for (int j = 1; j < qai.intents.size(); ++j) {
            String intent = qai.intents.get(j);
            intentScoreLists.add(processQuery(intent, searchInputDocSize, model)); 
          }
          qai.setScoreLists(queryScoreList, intentScoreLists);
        }
      } catch (IOException ex) {
        ex.printStackTrace();
      } finally {
        input.close();
      }
    }

    
    
    for (QueryAndIntents qai : qaiList) {
      
      qai.normalize(maxInputDocs);
      // System.out.println("WTF: " + maxInputDocs + ", LARGEST: " + qai.largest);
      // qai.print();

      int numIntents = qai.intentScoreLists.size() - 1;
      int limit = Integer.min(maxInputDocs, qai.queryScoreList.size()); 
      double pqiq = 1.0 / ((double)numIntents);
      ScoreList result = new ScoreList();
      Set<Integer> rankedDocs = new HashSet<>();
      Set<Integer> docsToLook = new HashSet<>();
      for (int i = 0; i < limit; ++i) {
        docsToLook.add(i);
      }
      // PM2
      if (parameters.get("diversity:algorithm").equals("PM2")) {
        ArrayList<Double> v = new ArrayList<Double>(Collections.nCopies(qai.intentScoreLists.size(), pqiq * ((double)limit)));
        ArrayList<Double> s = new ArrayList<Double>(Collections.nCopies(qai.intentScoreLists.size(), 0.0));
        ArrayList<Double> q = new ArrayList<Double>(Collections.nCopies(qai.intentScoreLists.size(), 0.0));

        while (result.size() < maxResultDocs && docsToLook.size() > 0) { 
          int bestIntent = 0;
          Double bestIntentScore = Double.MIN_VALUE;
          for (int j = 1; j < qai.intentScoreLists.size(); ++j) {
            double intentScore = v.get(j) / (2.0 * s.get(j) + 1.0);
            q.set(j, intentScore);
            if (intentScore > bestIntentScore) {
              bestIntent = j; 
              bestIntentScore = intentScore;
            }
          }
          SortedMap<Double, IndPair> best = new TreeMap<>();
          for (Integer i : docsToLook) {
            int docid = qai.queryScoreList.getDocid(i);
            double cover = lambda * bestIntentScore * qai.getIntentScore(docid, bestIntent); 
            double otherCoverSum = 0.0;
            for (int j = 1; j < qai.intentScoreLists.size(); ++j) {
              if (j != bestIntent) {
                otherCoverSum +=  q.get(j) * qai.getIntentScore(docid, j); 
              }
            }
            double otherCover = otherCoverSum * (1.0 - lambda);
            double score = cover + otherCover;
            best.put(score, new IndPair(docid, i));
          }

          double bestScore = best.lastKey();
          int bestDocid = best.get(bestScore).docid;
          int bestIndex = best.get(bestScore).index;
          docsToLook.remove(bestIndex);
          rankedDocs.add(bestDocid);
          result.add(bestDocid, bestScore);

          double bestDocIntentSum = 0.0;
          for (int j = 1; j < qai.intentScoreLists.size(); ++j) { 
            bestDocIntentSum += qai.getIntentScore(bestDocid, j);
          }

          for (int j = 1; j < qai.intentScoreLists.size(); ++j) { 
            double prevS = s.get(j);
            s.set(j, prevS + (qai.getIntentScore(bestDocid, j) / bestDocIntentSum)); 
          }
        }

        Double prev = null;
        for (int i = 0; i < result.size(); i++) {
          double score = result.getDocidScore(i);
          if (prev == null) {
            prev = score;
          } else if (score >= prev) {
            result.setDocidScore(i, score * 0.999);
          }
        }
      }
      // xQuad
      else {
        while (result.size() < maxResultDocs && docsToLook.size() > 0) {
          SortedMap<Double, IndPair> best = new TreeMap<>();
          // System.out.println("Finding the position: " + (result.size() + 1) + " best doc..."); 
          for (Integer i : docsToLook) {
            int docid = qai.queryScoreList.getDocid(i);
            double relevance = (1.0 - lambda) * qai.getQueryScore(i); 
            double intentSum = 0.0;
            for (int j = 1 ; j < qai.intentScoreLists.size(); ++j) {
              double divScore = pqiq * qai.getIntentScore(docid, j); 
              if (rankedDocs.size() > 0)  {
                for (Integer  rankedDocid : rankedDocs) {
                  divScore *= (1.0 - qai.getIntentScore(rankedDocid, j));  
                }
              }
              intentSum += divScore;
            } 
            double diversity = lambda * intentSum;
            double score = relevance + diversity;
            best.put(score, new IndPair(docid, i));
          }
          double bestScore = best.lastKey();
          int bestDocid = best.get(bestScore).docid;
          int bestIndex = best.get(bestScore).index;
          docsToLook.remove(bestIndex);
          rankedDocs.add(bestDocid);
          result.add(bestDocid, bestScore);
        }
      }
      if (trecEvalOutputLen < maxResultDocs) {
        result.truncate(trecEvalOutputLen); 
      } 
      printResults(qai.qid, result, trecEvalOutputPath); 
    }
  }


  private static Set<Integer> findFeaturesToDisable (Map<String, String> parameters) throws NumberFormatException {
    if (!parameters.containsKey("ltr:featureDisable")) {
      return new HashSet<Integer>();
    }
    String toIgnore = parameters.get("ltr:featureDisable");
    ArrayList<Integer> ignoredFeatures = new ArrayList<>(); 
    String[] each = toIgnore.split(","); 
    for (String s : each) {
      ignoredFeatures.add(Integer.parseInt(s)); 
    }
    return new HashSet<Integer>(ignoredFeatures);
  }

  static Map<String, ArrayList<FeatureVectorFileLine>> initializeFeatureVectors(String relevancePath) throws Exception {
    Map<String, ArrayList<FeatureVectorFileLine>> queryToDocs = new HashMap<String, ArrayList<FeatureVectorFileLine>>();
    
    File parameterFile = new File (relevancePath);

    if (! parameterFile.canRead ()) {
      throw new IllegalArgumentException
        ("Can't read " + relevancePath);
    }

    Scanner scan = new Scanner(parameterFile);
    
    ArrayList<FeatureVectorFileLine> fvflList = new ArrayList<>();
    
    String line = null;
    String currentQueryId = null;
    
    do {
      line = scan.nextLine();
      String[] items = line.split (" ");
      String queryId = items[0];
      String externalDocid = items[2];
      int docid = Idx.getInternalDocid(externalDocid);
      int relevanceScore = Integer.parseInt(items[3]);
      
      if (relevanceScore == -2) {
        relevanceScore = 0; 
      }
      
      if (currentQueryId == null) {
        currentQueryId = queryId;
      } else if (!currentQueryId.equals(queryId)) {
        queryToDocs.put(currentQueryId, fvflList);
        fvflList = new ArrayList<FeatureVectorFileLine>();
        currentQueryId = queryId;
      } 
      
      // Calculate feature vector
      FeatureVector fv = new FeatureVector();
      fvflList.add(new FeatureVectorFileLine(relevanceScore, docid, externalDocid, queryId, fv));
      
    } while (scan.hasNext()); 

    if (!fvflList.isEmpty()) {
      queryToDocs.put(currentQueryId, fvflList);
    }

    return queryToDocs;
  }

  private static class ScoreAndCount {
    public Double scoreBM25; 
    public Double scoreIndri; 
    public Double count;
    public Double idfWeightedMatch;
    public Double minDistanceBetween;
    public Double documentVectorLength;
    public Double documentQueryVectorInnerProduct;
    public Double rarestZipfRank;

    public ScoreAndCount () {
      this.scoreBM25 = null; 
      this.scoreIndri = null;
      this.count = null;
      this.idfWeightedMatch = null;
      this.minDistanceBetween = null;
      this.documentVectorLength = null;
      this.documentQueryVectorInnerProduct = null;
      this.rarestZipfRank = null; 
    }

    public ScoreAndCount(Double scoreBM25, Double scoreIndri, Double count, Double idfWeightedMatch,
                         Double minDistanceBetween, Double documentVectorLength, Double documentQueryVectorInnerProduct,
                         Double rarestZipfRank) {
      this.scoreBM25 = scoreBM25; 
      this.scoreIndri = scoreIndri;
      this.count = count;
      this.idfWeightedMatch = idfWeightedMatch;
      this.minDistanceBetween = minDistanceBetween;
      this.documentVectorLength = documentVectorLength;
      this.documentQueryVectorInnerProduct = documentQueryVectorInnerProduct;
      this.rarestZipfRank = rarestZipfRank;
    }
  } 


  static Double scoreBM25 (RetrievalModelBM25 rm, double tf, double df, double doc_len, double avg_doc_len, double num_docs) {
    double p1 = Math.max(0.0, Math.log(((num_docs - df + 0.5) / (df + 0.5))));
    double p2 = tf / (tf + rm.k_1 * ((1.0 - rm.b) + rm.b * (doc_len / avg_doc_len)));
    double p3 = (rm.k_3 + 1.0) / (rm.k_3 + 1.0);
    return p1*p2*p3;
  } 

  static Double scoreIndri (RetrievalModelIndri rm, double tf, double ctf, double doc_len, double total_field_len, double num_docs) {
    double pqc = ctf == 0.0 ? (0.5 / total_field_len) : (ctf / total_field_len);
    double p1 = (1.0 - rm.lambda) * ((tf + (rm.mu * pqc)) / (doc_len + rm.mu));
    double p2 = rm.lambda * pqc;
    return p1 + p2;
  }

  private static ScoreAndCount featurePair(RetrievalModelBM25 bm25, 
                                           RetrievalModelIndri indri, 
                                           Map<String, Integer> queryTokens, 
                                           int docid, 
                                           String field, 
                                           double total_field_len,
                                           double avg_doc_len, 
                                           double num_docs,
                                           double num_docs_field,
                                           double num_words,
                                           Map<String, Double> ctfs) throws IOException {

    double scoreBM25 = 0.0;
    Double scoreIndri = 1.0;
    Double rarestZipfRank = 0.0;
    int count = 0;
    TermVector tv = new TermVector(docid, field); 

    if (tv.stemsLength() == 0 && tv.positionsLength() == 0) {
      return new ScoreAndCount();
    }

    double doc_len = (double)tv.positionsLength();
    double num_tokens = (double)queryTokens.size();
    
    Double weightedIdf = 0.0;
    Double docVectorMag = 0.0;
    Double innerProd = 0.0;
    Double minDistanceScore = 0.0;

    Set<String> matches = new HashSet<String>();

    for (int i = 0; i < tv.stemsLength(); ++i) {
      String stem = tv.stemString(i); 
      if (stem == null) {
        continue;
      }
      docVectorMag += (tv.stemFreq(i) * tv.stemFreq(i)); 
      if (queryTokens.containsKey(stem) && tv.indexOfStem(stem) != -1) {
        matches.add(stem);

        String combined = stem + "." + field;
        double ctf = 0.0;
        if (ctfs.containsKey(combined)) {
          ctf = ctfs.get(combined);
        } else {
          ctf = (double)Idx.getTotalTermFreq(field, stem);
          ctfs.put(combined, ctf);
        }
        
        double tf = (double)tv.stemFreq(i);
        double df = Idx.getDocFreq(field, stem); 
        double idf = Math.log(num_docs / ((double)Idx.getDocFreq(field, stem)));

        weightedIdf += idf * tf;
        scoreBM25 += scoreBM25(bm25, tf, df, doc_len, avg_doc_len, num_docs);
        double termScoreIndri = scoreIndri(indri, tf, ctf, doc_len, total_field_len, num_docs);
        scoreIndri *= Math.pow(termScoreIndri, (1.0 / num_tokens));
        count += 1;

        rarestZipfRank = Math.max(rarestZipfRank, Math.log((tf * 0.1 * num_words) / ctf));
        innerProd += (tf * queryTokens.get(stem));
      }
    }

    if (count == 0) {
      scoreIndri = 0.0;
      rarestZipfRank = null;
      minDistanceScore = null;
    } else if (field == "title") {
      Integer minDistance = Integer.MAX_VALUE;
      int total = 0;
      int prev = -1;
      for (int i = 0 ; i < tv.positionsLength(); ++i) {
        if (matches.contains(tv.stemAt(i))) {
          total++;
          if (prev == -1) {
            prev = i;
          } else {
            minDistance = Integer.min(minDistance, i - prev);
            prev = i;
          }
        } 
      }
      if (total == 1) {
        minDistanceScore = 0.0;
      } else {
        minDistanceScore = 1.0 / ((double)minDistance);
      }
    } else if (field == "body") {
      rarestZipfRank /= tv.positionsLength(); 
    } 
    /*
     * 
     * 
     * Double scoreBM25, Double scoreIndri, Double count, Double idfWeightedMatch,
                         Double minDistanceBetween, Double documentVectorLength, Double documentQueryVectorInnerProduct,
                         Double rarestZipfRank) {
     */
    return new ScoreAndCount(scoreBM25, scoreIndri, (double)count, weightedIdf, 
                             minDistanceScore, docVectorMag, innerProd, rarestZipfRank);
  }

  static void normalizeFeatureValues(ArrayList<FeatureVectorFileLine> fvfls) {
    for (int i = 1; i < QryEval.LAST_FEATURE; ++i) {
      Double min = null;
      Double max = null;
      for (FeatureVectorFileLine fvfl : fvfls) {
        Double cur = fvfl.fv.getFeature(i);
        if (cur != null) {
          if (min == null) {
            min = cur;
            max = cur;
          } else {
            min = Double.min(min, cur); 
            max = Double.max(max, cur); 
          }
        }
      }
      // System.out.println(i + ":MAX: " + max + " MIN:" + min);
      Double diff = min == null ? null : max - min; 

      for (FeatureVectorFileLine fvfl : fvfls) {
        Double cur = fvfl.fv.getFeature(i);
        if (cur != null && diff != 0.0) {
          fvfl.fv.setFeature(i, (cur - min) / diff); 
        }
        else {
          cur = 0.0; 
        }
      }
    }
     
  } 

  static ArrayList<FeatureVectorFileLine> createFeatureVectorFile (String outputPath,                  
                                       String inputPath,
                                       boolean isSVMRank, 
                                       RetrievalModelIndri indri,
                                       RetrievalModelBM25 bm25,
                                       Map<String, ArrayList<FeatureVectorFileLine>> mappings,
                                       Set<Integer> disabledFeatures,
                                       String topN, 
                                       boolean testQueries) throws Exception {
   
    File inputFile = new File (inputPath);

    if (! inputFile.canRead ()) {
      throw new IllegalArgumentException
        ("Can't read " + inputPath);
    }

    Scanner scan = new Scanner(inputFile);
    
    String line = null;
    

    double num_docs_n = (double)Idx.getNumDocs();
    double num_docs_body = (double)(Idx.getDocCount("body"));
    double num_docs_title = (double)(Idx.getDocCount("title"));
    double num_docs_url = (double)(Idx.getDocCount("url"));
    double num_docs_inlink = (double)(Idx.getDocCount("inlink"));

    double total_field_len_body = (double)(Idx.getSumOfFieldLengths("body"));
    double total_field_len_title = (double)(Idx.getSumOfFieldLengths("title"));
    double total_field_len_url = (double)(Idx.getSumOfFieldLengths("url"));
    double total_field_len_inlink = (double)(Idx.getSumOfFieldLengths("inlink"));
    double total_words = total_field_len_body + total_field_len_title + total_field_len_url + total_field_len_inlink;

    double avg_doc_len_body = total_field_len_body / num_docs_body;
    double avg_doc_len_title = total_field_len_title / num_docs_title;
    double avg_doc_len_url = total_field_len_url / num_docs_url;
    double avg_doc_len_inlink = total_field_len_inlink / num_docs_inlink;

    FileWriter myWriter = new FileWriter(outputPath, true);
    ArrayList<FeatureVectorFileLine> fileLines = new ArrayList<>();

    Map<String, Double> ctfs = new HashMap<String, Double>();
    
    do {
      line = scan.nextLine();
      String[] items = line.split (":");
      String queryId = items[0];
      String query = items[1];
      String[] tokens = QryParser.tokenizeString(query);
      Map<String, Integer> queryVector = new HashMap<String, Integer>();

      for (String token : tokens) {
        int old = 0;
        if (queryVector.containsKey(token)) {
          old = queryVector.get(token);
        } 
        queryVector.put(token, old + 1);
      }
      int totalQueryVectorMagnitude = 0;
      for (Integer i : queryVector.values()) {
        totalQueryVectorMagnitude += i * i;
      }
      double queryVectorLength = Math.sqrt((double)totalQueryVectorMagnitude);



      ArrayList<FeatureVectorFileLine> fvfls;
      if (testQueries) {
        fvfls = new ArrayList<>();
        ScoreList sl = processQuery(query, topN, bm25);
        for (int i = 0; i < sl.size(); ++i) {
          int docid = sl.getDocid(i); 
          String externalDocid = Idx.getExternalDocid(docid);
          FeatureVector fv = new FeatureVector();
          fvfls.add(new FeatureVectorFileLine(0, docid, externalDocid, queryId, fv));
        }
      } else {
        fvfls = mappings.get(queryId);
      }
      
      for (FeatureVectorFileLine fvfl : fvfls) {
        int docid = fvfl.internalDocid;

        String spamAttrib = Idx.getAttribute("spamScore", docid);
        String rawUrl = Idx.getAttribute ("rawUrl", docid);
        String pgRank = Idx.getAttribute ("PageRank", docid);

        Double spamScore = spamAttrib == null ? null : Double.parseDouble(spamAttrib);
        Double urlDepth = rawUrl == null ? null : (double)(rawUrl.chars().filter(num -> num == '/').count());
        Double fromWikipedia = rawUrl == null ? null : (rawUrl.contains("wikipedia.org") ? 1.0 : 0.0);
        Double prScore = pgRank == null ? null : Double.parseDouble(pgRank);

        ScoreAndCount sacBody = featurePair(bm25, indri, queryVector, docid, "body", total_field_len_body, avg_doc_len_body, num_docs_n, num_docs_body, total_words, ctfs);
        ScoreAndCount sacTitle = featurePair(bm25, indri, queryVector, docid, "title", total_field_len_title, avg_doc_len_title, num_docs_n, num_docs_title, total_words, ctfs);
        ScoreAndCount sacUrl = featurePair(bm25, indri, queryVector, docid, "url", total_field_len_url, avg_doc_len_url, num_docs_n, num_docs_url, total_words, ctfs);
        ScoreAndCount sacInlink = featurePair(bm25, indri, queryVector, docid, "inlink", total_field_len_inlink, avg_doc_len_inlink, num_docs_n, num_docs_inlink, total_words, ctfs);


        fvfl.fv.setFeature(1, spamScore);
        fvfl.fv.setFeature(2, urlDepth);
        fvfl.fv.setFeature(3, fromWikipedia);
        fvfl.fv.setFeature(4, prScore);
        fvfl.fv.setFeature(5, sacBody.scoreBM25);
        fvfl.fv.setFeature(6, sacBody.scoreIndri);
        fvfl.fv.setFeature(7, sacBody.count);
        fvfl.fv.setFeature(8, sacTitle.scoreBM25);
        fvfl.fv.setFeature(9, sacTitle.scoreIndri);
        fvfl.fv.setFeature(10, sacTitle.count);
        fvfl.fv.setFeature(11, sacUrl.scoreBM25);
        fvfl.fv.setFeature(12, sacUrl.scoreIndri);
        fvfl.fv.setFeature(13, sacUrl.count);
        fvfl.fv.setFeature(14, sacInlink.scoreBM25);
        fvfl.fv.setFeature(15, sacInlink.scoreIndri);
        fvfl.fv.setFeature(16, sacInlink.count);

        // idf weighitng
        fvfl.fv.setFeature(17, sacUrl.idfWeightedMatch);

        // Cosine similarity
        if (sacTitle.documentVectorLength == null) {
          fvfl.fv.setFeature(18, null);
        } else {
          double titleCosineSimilarity = 
          sacTitle.documentQueryVectorInnerProduct / (sacTitle.documentVectorLength * queryVectorLength);
          fvfl.fv.setFeature(18, titleCosineSimilarity);
        }
        
        // Min distance 
        fvfl.fv.setFeature(19, sacTitle.minDistanceBetween);

        // Rarest zipfs law 
        fvfl.fv.setFeature(20, sacBody.rarestZipfRank);
      } 

      if (isSVMRank) {
          
        normalizeFeatureValues(fvfls);
      }
     
      for (FeatureVectorFileLine fvfl : fvfls) { 
        String line2 = fvfl.toString(isSVMRank, disabledFeatures);
        // System.out.println("LINE: " + line2);
        myWriter.write(line2);
        fileLines.add(fvfl);
      }

    } while (scan.hasNext()); 

    myWriter.close();
    scan.close();
    return fileLines;
  }

  private static void sortAndOutputFinalLtrResults (String outputPath,
                                                    String inputPath,
                                                    boolean isSVMRank,
                                                    ArrayList<FeatureVectorFileLine> fileLines,
                                                    int topN) throws Exception {
    File inputFile = new File (inputPath);

    if (! inputFile.canRead ()) {
      throw new IllegalArgumentException
        ("Can't read " + inputPath);
    }

    Scanner scan = new Scanner(inputFile);
    
    String line = null;
    


    FileWriter myWriter = new FileWriter(outputPath, true);
    String currentQueryId = null;
    ScoreList currentScoreList = new ScoreList();
    int i = 0;
    do {
      line = scan.nextLine();
      FeatureVectorFileLine fvfl = fileLines.get(i);
      int docid = fvfl.internalDocid;
      if (currentQueryId == null) {
        currentQueryId = fvfl.queryId; 
      } else if (currentQueryId != fvfl.queryId) {
        currentScoreList.sort(); 
        currentScoreList.truncate(topN); 
        printResults(currentQueryId, currentScoreList, outputPath);
        currentQueryId = fvfl.queryId;
        currentScoreList = new ScoreList();
      }
      double score = 0.0;
      if (isSVMRank) {
        String strippedLine = line.strip();
        score = Double.parseDouble(strippedLine);
      } else {
        String[] items = line.strip().split ("\\s+");
        String lineQueryId = items[0];
        String lineScore = items[2];
        score = Double.parseDouble(lineScore);
      }
      currentScoreList.add(docid, score);

      i++;
        // myWriter.write(fvfl.toString(isSVMRank, disabledFeatures));
    } while (scan.hasNext()); 

    if (currentScoreList.size() != 0) {
      printResults(currentQueryId, currentScoreList, outputPath); 
    }

    myWriter.close();
    scan.close();                    
  }

  private static void performLearningToRank (Map<String, String> parameters) 
    throws Exception{
    
    Set<Integer> disabledFeatures = findFeaturesToDisable(parameters);
    RetrievalModelIndri indri = new RetrievalModelIndri(Integer.parseInt(parameters.get("Indri:mu")), 
                                                   Double.parseDouble(parameters.get("Indri:lambda")));

    RetrievalModelBM25 bm25 = new RetrievalModelBM25(Double.parseDouble(parameters.get("BM25:k_1")),
                                                 Double.parseDouble(parameters.get("BM25:b")),
                                                 Double.parseDouble(parameters.get("BM25:k_3")));

    Map<String, ArrayList<FeatureVectorFileLine>> mappings = initializeFeatureVectors(parameters.get("ltr:trainingQrelsFile"));

    boolean isSVMRank = parameters.get("ltr:toolkit").strip().toLowerCase().equals("svmrank");

    
    createFeatureVectorFile(parameters.get("ltr:trainingFeatureVectorsFile"),
                            parameters.get("ltr:trainingQueryFile"),
                            isSVMRank,
                            indri,
                            bm25,
                            mappings,
                            disabledFeatures,
                            parameters.get("trecEvalOutputLength"),
                            false); 
    // train
    if (isSVMRank) {
      Utils.runExternalProcess (
      "svm_rank_learn", 
      new String[] {
          parameters.get("ltr:svmRankLearnPath"), // svmRankLearnPath,            // From the parameter file
          "-c", parameters.get("ltr:svmRankParamC"), // svmRankParamC,         // From the parameter file
          parameters.get("ltr:trainingFeatureVectorsFile"), // TrainingFeatureVectorsFile,  // From the parameter file
          parameters.get("ltr:modelFile") } );               // From the parameter file 
    } else {
      int ranker = Integer.parseInt(parameters.get("ltr:RankLib:model"));
      if (ranker == 4) {
        Evaluator.main (
        new String[] {
          "-ranker", parameters.get("ltr:RankLib:model"),              // From the parameter file
          "-metric2t", parameters.get("ltr:RankLib:metric2t"),         // From the parameter file
          "-train", parameters.get("ltr:trainingFeatureVectorsFile"),  // From the parameter file
          "-save", parameters.get("ltr:modelFile") } );                // From the parameter file
      } else {
        Evaluator.main (
        new String[] {
            "-ranker", parameters.get("ltr:RankLib:model"),              // From the parameter file
            "-train", parameters.get("ltr:trainingFeatureVectorsFile"),  // From the parameter file
            "-save", parameters.get("ltr:modelFile") } );                // From the parameter file
      }
      
    }

    ArrayList<FeatureVectorFileLine> fileLines = 
                            createFeatureVectorFile(parameters.get("ltr:testingFeatureVectorsFile"),
                            parameters.get("queryFilePath"),
                            isSVMRank,
                            indri,
                            bm25,
                            mappings,
                            disabledFeatures,
                            parameters.get("trecEvalOutputLength"),
                            true);

    if (isSVMRank) {
      Utils.runExternalProcess (
      "svm_rank_learn", 
      new String[] {
          parameters.get("ltr:svmRankClassifyPath"), // svmRankLearnPath,            // From the parameter file
          parameters.get("ltr:testingFeatureVectorsFile"), // TrainingFeatureVectorsFile,  // From the parameter file
          parameters.get("ltr:modelFile"),
          parameters.get("ltr:testingDocumentScores"),
          "-c", parameters.get("ltr:svmRankParamC"), } );               // From the parameter file 
    } else {
      int ranker = Integer.parseInt(parameters.get("ltr:RankLib:model"));
      if (ranker == 4) {
        Evaluator.main (
        new String[] {
          "-load", parameters.get("ltr:modelFile"),              // From the parameter file
          "-metric2t", parameters.get("ltr:RankLib:metric2t"),         // From the parameter file
          "-rank", parameters.get("ltr:testingFeatureVectorsFile"),  // From the parameter file
          "-score", parameters.get("ltr:testingDocumentScores") } );                // From the parameter file
      } else {
        Evaluator.main (
        new String[] {
            "-load", parameters.get("ltr:modelFile"),              // From the parameter file
            "-rank", parameters.get("ltr:testingFeatureVectorsFile"),  // From the parameter file
            "-score", parameters.get("ltr:testingDocumentScores") } );                // From the parameter file
      }
      /* 
      // if (ranker == 4) {
        Evaluator.main (
        new String[] {
          "-load", parameters.get("ltr:RankLib:model"),              // From the parameter file
          "-rank", parameters.get("ltr:testingFeatureVectorsFile"),  // From the parameter file
          "-score", parameters.get("ltr:testingDocumentScores") } );                // From the parameter file
      // }
       */
    } 

    int topN = Integer.parseInt(parameters.get("trecEvalOutputLength"));
    sortAndOutputFinalLtrResults(parameters.get("trecEvalOutputPath"), 
                                 parameters.get("ltr:testingDocumentScores"),
                                 isSVMRank,
                                 fileLines,
                                 topN);
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
        // System.out.println("NO EXPANSION TERMS? ");
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
