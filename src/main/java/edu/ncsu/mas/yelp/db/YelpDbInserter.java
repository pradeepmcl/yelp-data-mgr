package edu.ncsu.mas.yelp.db;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class YelpDbInserter implements AutoCloseable {

  private static final String mysqlDriver = "com.mysql.jdbc.Driver";
  private final String mDbUrl;
  private final Connection mConn;

  public YelpDbInserter(String dbUrl) throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, SQLException {
    Class.forName(mysqlDriver).newInstance();
    mDbUrl = dbUrl;
    mConn = DriverManager.getConnection(mDbUrl);
  }

  @Override
  public void close() throws SQLException {
    if (mConn != null) {
      mConn.close();
    }
  }

  private final DateFormat userDateFormatter = new SimpleDateFormat("yyyy-MM");

  private final String userInsertQuery = "INSERT INTO User(id_original_usr, name_usr, "
      + "yelping_since_usr, review_count_usr, fans_usr, type_usr, avg_stars_usr, votes_funny_usr, "
      + "votes_useful_usr, votes_cool_usr, compliments_profile_usr, compliments_cute_usr, "
      + "compliments_funny, compliments_plain_usr, compliments_writer_usr, compliments_note_usr, "
      + "compliments_photos_usr, compliments_hot_usr, compliments_cool_usr, compliments_more_usr) "
      + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private int setUserParameterValues(PreparedStatement prpdStmt, JSONObject userOb)
      throws JSONException, SQLException, ParseException {
    prpdStmt.setString(1, userOb.getString("user_id"));
    prpdStmt.setString(2, userOb.getString("name"));
    prpdStmt.setDate(3, new Date(userDateFormatter.parse(userOb.getString("yelping_since")).getTime()));
    prpdStmt.setInt(4, userOb.getInt("review_count"));
    prpdStmt.setInt(5, userOb.getInt("fans"));
    prpdStmt.setString(6, userOb.getString("type"));
    prpdStmt.setDouble(7, userOb.getDouble("average_stars"));

    JSONObject votesOb = userOb.getJSONObject("votes");
    String[] voteTypes = { "funny", "useful", "cool" };
    for (int i = 8; i <= 10; i++) {
      if (votesOb.has(voteTypes[i - 8])) {
        prpdStmt.setInt(i, votesOb.getInt(voteTypes[i - 8]));
      } else {
        prpdStmt.setNull(i, Types.INTEGER);
      }
    }

    JSONObject complimentsOb = userOb.getJSONObject("votes");
    String[] complimentTypes = { "profile", "cute", "funny", "plain", "writer", "note", "photos",
        "hot", "cool", "more" };
    for (int i = 11; i <= 20; i++) {
      if (complimentsOb.has(complimentTypes[i - 11])) {
        prpdStmt.setInt(i, complimentsOb.getInt(complimentTypes[i - 11]));
      } else {
        prpdStmt.setNull(i, Types.INTEGER);
      }
    }
    
    prpdStmt.addBatch();
    return 1;
  }
  
  private final String businessInsertQuery = "INSERT INTO Business(id_original_bus, name_bus, "
      + "type_bus, city_bus, state_bus, address_bus, open_bus, review_count_bus, stars_bus, "
      + "latitude_bus, longitude_bus) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  
  private int setBusinessParameterValues(PreparedStatement prpdStmt, JSONObject busOb)
      throws JSONException, SQLException, ParseException {
    String[] fieldNames = { "business_id", "name", "type", "city", "state" };
    for (int i = 0; i < fieldNames.length; i++) {
      prpdStmt.setString(i+1, busOb.getString(fieldNames[i]));
    }
    
    if (busOb.has("address")) {
      prpdStmt.setString(6, busOb.getString("address"));
    } else {
      prpdStmt.setNull(6, Types.VARCHAR);
    }
    
    prpdStmt.setBoolean(7, busOb.getBoolean("open"));
    prpdStmt.setInt(8, busOb.getInt("review_count"));
    prpdStmt.setDouble(9, busOb.getDouble("stars"));
    prpdStmt.setDouble(10, busOb.getDouble("latitude"));
    prpdStmt.setDouble(11, busOb.getDouble("longitude"));
    
    prpdStmt.addBatch();
    return 1;
  }
  
  private final DateFormat reviewDateFormatter = new SimpleDateFormat("yyyy-MM-dd");
  
  private final String reviewInsertQuery = "INSERT INTO Review_XX(id_original_raz, user_id_raz, "
      + "bus_id_raz, date_raz, type_raz, stars_raz, votes_funny_raz, votes_useful_raz, "
      + "votes_cool_raz, text_raz) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  
  private int setReviewParameterValues(PreparedStatement prpdStmt, JSONObject reviewOb,
      Map<String, Integer> userIdMap, Map<String, Integer> busIdMap) throws JSONException,
      SQLException, ParseException {
    if (!busIdMap.containsKey(reviewOb.get("business_id"))) {
      return 0;
    }
    
    prpdStmt.setString(1, reviewOb.getString("review_id"));
    prpdStmt.setInt(2, userIdMap.get(reviewOb.get("user_id")));
    prpdStmt.setInt(3, busIdMap.get(reviewOb.get("business_id")));
    prpdStmt.setDate(4, new Date(reviewDateFormatter.parse(reviewOb.getString("date")).getTime()));
    prpdStmt.setString(5, reviewOb.getString("type"));
    prpdStmt.setDouble(6, reviewOb.getDouble("stars"));
    
    JSONObject votesOb = reviewOb.getJSONObject("votes");
    String[] voteTypes = { "funny", "useful", "cool" };
    for (int i = 7; i <= 9; i++) {
      prpdStmt.setInt(i, votesOb.getInt(voteTypes[i - 7]));
    }
    
    prpdStmt.setString(10, reviewOb.getString("text"));
    
    prpdStmt.addBatch();
    return 1;
  }
  
  private final String businessCategoriesInsertQuery = "INSERT INTO Business_Categories("
      + "bus_id_cat, category_cat) VALUES(?, ?)";
  
  // TODO
  private int setBusinessCategoriesParameterValues(PreparedStatement prpdStmt,
      JSONObject busOb, Map<String, Integer> busIdMap) throws JSONException, SQLException,
      ParseException {
    prpdStmt.setInt(1, busIdMap.get(busOb.getString("business_id")));
    if (busOb.has("categories")) {
      JSONArray catJsonArray = busOb.getJSONArray("categories");
      int insertCount = 0;
      for (int i = 0; i < catJsonArray.length(); i++) {
        // System.out.println(catJsonArray.getString(i));
        prpdStmt.setString(2, catJsonArray.getString(i));
        prpdStmt.addBatch();
        insertCount++;
      }
      return insertCount;
    }
    return 0;
  }
  
  private String getInsertQuery(String tableName) {
    tableName = tableName.toLowerCase();
    switch (tableName) {
    case "user":
      return userInsertQuery;
    case "business":
      return businessInsertQuery;
    case "review_az":
      return reviewInsertQuery.replaceFirst("Review_XX", "Review_AZ");
    case "review_nv":
      return reviewInsertQuery.replaceFirst("Review_XX", "Review_NV");
    case "review_nc":
      return reviewInsertQuery.replaceFirst("Review_XX", "Review_NC");
    case "business_categories":
      return businessCategoriesInsertQuery;
    default:
      throw new IllegalArgumentException("Unknown table: " + tableName);
    }
  }

  private int setParameterValues(String tableName, PreparedStatement prpdStmt, JSONObject jsonOb)
      throws JSONException, SQLException, ParseException {
    tableName = tableName.toLowerCase();
    switch (tableName) {
    case "user":
      return setUserParameterValues(prpdStmt, jsonOb);
    case "business":
      return setBusinessParameterValues(prpdStmt, jsonOb);
    case "review_az":
    case "review_nv":
    case "review_nc":
      return setReviewParameterValues(prpdStmt, jsonOb, userIdMap, busIdMap);
    case "business_categories":
      return setBusinessCategoriesParameterValues(prpdStmt, jsonOb, busIdMap);
    default:
      throw new IllegalArgumentException("Unknown table: " + tableName);
    }
  }
  
  private final Map<String, Integer> userIdMap = new HashMap<String, Integer>();
  
  private String userIdSelectQuery = "SELECT id_original_usr, id_usr FROM User";
  
  private void pupulateUserIdMap() throws SQLException {
    try (PreparedStatement stmt = mConn.prepareStatement(userIdSelectQuery);
        ResultSet rs = stmt.executeQuery()) {
      while (rs.next()) {
        userIdMap.put(rs.getString(1), rs.getInt(2));
      }
    }
  }
  
  private final Map<String, Integer> busIdMap = new HashMap<String, Integer>();
  
  private String busIdSelectQuery = "SELECT id_original_bus, id_bus FROM Business "
      + "where state_bus = ?";
  
  private void pupulateBusIdMap(String tableName) throws SQLException {
    try (PreparedStatement stmt = mConn.prepareStatement(busIdSelectQuery)) {
      switch (tableName) {
      case "review_az":
        stmt.setString(1, "AZ");
        break;
      case "review_nv":
        stmt.setString(1, "NV");
        break;
      case "review_nc":
        stmt.setString(1, "NC");
        break;
      default:
        throw new IllegalArgumentException("Unknown table: " + tableName);
      }

      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          busIdMap.put(rs.getString(1), rs.getInt(2));
        }
      }
    }
  }
  
  private String allBusIdSelectQuery = "SELECT id_original_bus, id_bus FROM Business";
  
  private void pupulateBusIdMap() throws SQLException {
    try (Statement stmt = mConn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery(allBusIdSelectQuery)) {
        while (rs.next()) {
          busIdMap.put(rs.getString(1), rs.getInt(2));
        }
      }
    }
  }

  public void insert(String filename, String tableName) throws FileNotFoundException, IOException,
      SQLException, JSONException, ParseException {
    if (tableName.startsWith("review")) {
      if (userIdMap.isEmpty()) {
        pupulateUserIdMap();
      }
      if (busIdMap.isEmpty()) {
        pupulateBusIdMap(tableName);
      }
    } else if (tableName.equalsIgnoreCase("business_categories")) {
      pupulateBusIdMap();
    }
    
    mConn.setAutoCommit(false);
    try (BufferedReader br = new BufferedReader(new FileReader(filename));
        PreparedStatement prpdStmt = mConn.prepareStatement(getInsertQuery(tableName))) {
      // Each line is in json format (e.g., resources/user_sample.json)
      int batchedSoFar = 0;
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        JSONObject jsonOb = new JSONObject(line);
        int batchCount = setParameterValues(tableName, prpdStmt, jsonOb);
        if (batchCount > 0) {
          batchedSoFar += batchCount;
          if (batchedSoFar > 10000) {
            prpdStmt.executeBatch();
            mConn.commit();
            System.out.println("Inserted in this batch: " + batchedSoFar);
            batchedSoFar = 0;
          }
        }
      }
      prpdStmt.executeBatch(); // Left overs
      mConn.commit();
      mConn.setAutoCommit(true);
    }
  }

  public static void main(String[] args) throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, SQLException, FileNotFoundException, IOException, JSONException,
      ParseException {
    String dbUrl = args[0];
    String tableName = args[1];
    String inFilename = args[2];
    try (YelpDbInserter inserter = new YelpDbInserter(dbUrl)) {
      inserter.insert(inFilename, tableName);
    }
  }
}
