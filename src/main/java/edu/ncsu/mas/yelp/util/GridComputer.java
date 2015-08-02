package edu.ncsu.mas.yelp.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Divides a set of geo coordinates into grids. Same as GridComputer, but the
 * input file is in a different format.
 * 
 * @author pmuruka
 *
 */
public class GridComputer {

  String inputFilename;
  String outputFilename;
  Double gridLength;
  
  ConcurrentMap<Long, Double[]> idToLatLon = new ConcurrentHashMap<Long, Double[]>();
  ConcurrentMap<Long, Long> idToTweetId = new ConcurrentHashMap<Long, Long>();
  ConcurrentNavigableMap<Double, Set<Long>> latToId = new ConcurrentSkipListMap<Double, Set<Long>>();
  ConcurrentNavigableMap<Double, Set<Long>> lonToId = new ConcurrentSkipListMap<Double, Set<Long>>();
  
  public GridComputer(String inputFilename, String outputFilename, Double gridLength) {
    this.inputFilename = inputFilename;
    this.outputFilename = outputFilename;
    this.gridLength = gridLength;
  }
  
  public static void main(String[] args) throws FileNotFoundException, IOException,
      InterruptedException, ExecutionException {
    String inFilename = args[0];
    String outFilename = args[1];
    Double gridLength = Double.parseDouble(args[2]);

    GridComputer gridComputer = new GridComputer(inFilename, outFilename, gridLength);

    gridComputer.readInputFile();
    
    gridComputer.divideIntoGrids();;
  }
  
  /**
   * 1 degree of latitude ~= 69 miles 
   * 1 degree of longitude ~= cos(latitude)*69 miles
   * 
   * Source: http://tinyurl.com/n4nfr95
   * @throws ExecutionException 
   * @throws InterruptedException 
   * 
   * @throws IOException 
   */
  private void divideIntoGrids() throws InterruptedException, ExecutionException, IOException {
    List<String> outList = Collections.synchronizedList(new ArrayList<String>());
    
    ExecutorService gridCompSer = Executors.newFixedThreadPool(Runtime.getRuntime()
        .availableProcessors());
    CompletionService<String> gridCompCompletionSerive = new ExecutorCompletionService<String>(
        gridCompSer);
    int submitCount = 100000;
    long taskCount = 0;
    
    // Compute bounding box
    Double lowestLat = latToId.firstKey();
    Double highestLat = latToId.lastKey();
    Double lowestLon = lonToId.firstKey();
    Double highestLon = lonToId.lastKey();

    Double southWestLat = lowestLat;
    Double southWestLon = lowestLon;
    do {
      Double northEastLat = southWestLat + (gridLength / 69);
      do {
        Double northEastLon = southWestLon
            + (gridLength / (Math.abs(Math.cos(Math.toRadians(southWestLon)) * 69)));

        gridCompCompletionSerive.submit(new GridComputationCallable(southWestLat, southWestLon,
            northEastLat, northEastLon/*, taskCount*/));
        if (++taskCount % submitCount == 0) {
          for (int i = 0; i < submitCount; i++) {
            String result = gridCompCompletionSerive.take().get();
            if (result.length() > 0) {
              outList.add(result);
            }
          }
          System.out.println("Completed " + taskCount + " tasks");
          try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputFilename,
              true)))) {
            Iterator<String> i = outList.iterator();
            while (i.hasNext()) {
              out.println(i.next());
            }
            outList.clear();
          }
        }
        southWestLon = northEastLon;
      } while (southWestLon <= highestLon);
      southWestLat = northEastLat;
      southWestLon = lowestLon;
    } while (southWestLat <= highestLat);
    
    for (int i = 0; i < taskCount % submitCount; i++) {
      outList.add(gridCompCompletionSerive.take().get());
    }
    System.out.println("Completed " + taskCount + " (all) tasks");
    try (PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputFilename,
        true)))) {
      Iterator<String> i = outList.iterator();
      while (i.hasNext()) {
        out.println(i.next());
      }
      outList.clear();
    }
    
    gridCompSer.shutdown();
  }
  
  private class GridComputationCallable implements Callable<String> {
    final Double southWestLat;
    final Double southWestLon;
    final Double northEastLat;
    final Double northEastLon;
    // final long taskNum;

    public GridComputationCallable(Double southWestLat, Double southWestLon, Double northEastLat,
        Double northEastLon/*, long taskNum*/) {
      this.southWestLat = southWestLat;
      this.southWestLon = southWestLon;
      this.northEastLat = northEastLat;
      this.northEastLon = northEastLon;
      // this.taskNum = taskNum;
    }

    @Override
    public String call() {
      Set<Long> pointsInGrid = findAllPointsInAGrid(southWestLat, southWestLon, northEastLat,
          northEastLon);
      StringBuilder result = new StringBuilder();
      if (pointsInGrid.size() > 0) {
        result.append(southWestLat + "," + southWestLon + "," + northEastLat + "," + northEastLon);
        for (Long pointInGrid : pointsInGrid) {
          Long tweetId = idToTweetId.get(pointInGrid);
          Double[] latLon = idToLatLon.get(pointInGrid);
          result.append("," + tweetId + "," + latLon[0] + "," + latLon[1]);
        }
      }
      return result.toString();
    }
  }
  
  private Set<Long> findAllPointsInAGrid(Double southWestLat, Double southWestLon,
      Double northEastLat, Double northEastLon) {
    Collection<Set<Long>> latIdCol = latToId.subMap(southWestLat, true, northEastLat, false)
        .values();
    Collection<Set<Long>> lonIdCol = lonToId.subMap(southWestLon, true, northEastLon, false)
        .values();

    Set<Long> latIdsSet = new HashSet<Long>();
    for (Set<Long> latIds : latIdCol) {
      latIdsSet.addAll(latIds);
    }

    Set<Long> lonIdsSet = new HashSet<Long>();
    for (Set<Long> lonIds : lonIdCol) {
      lonIdsSet.addAll(lonIds);
    }

    latIdsSet.retainAll(lonIdsSet);
    return latIdsSet;
  }

  private void readInputFile() throws FileNotFoundException, IOException {
    try (BufferedReader br = new BufferedReader(new FileReader(inputFilename))) {
      String line = br.readLine(); // Skip first line (header)
      line = br.readLine();
      
      Long id = 0L;
      while (line != null) {
        String[] lineParts = line.split(",|;");
        
        idToTweetId.put(id, Long.parseLong(lineParts[0]));
        
        Double lat = Double.parseDouble(lineParts[1]);
        Double lon = Double.parseDouble(lineParts[2]);

        idToLatLon.put(id, new Double[] { lat, lon });

        Set<Long> latIds = latToId.get(lat);
        if (latIds == null) {
          latIds = new HashSet<Long>();
          latToId.put(lat, latIds);
        }
        latIds.add(id);

        Set<Long> lonIds = lonToId.get(lon);
        if (lonIds == null) {
          lonIds = new HashSet<Long>();
          lonToId.put(lon, lonIds);
        }
        lonIds.add(id);

        id++;
        line = br.readLine();
      }
    }
  }
}
