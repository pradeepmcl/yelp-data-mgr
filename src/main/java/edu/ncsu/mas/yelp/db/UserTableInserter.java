package edu.ncsu.mas.yelp.db;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.json.JSONException;
import org.json.JSONObject;

public class UserTableInserter implements AutoCloseable {

  private static final String mysqlDriver = "com.mysql.jdbc.Driver";
  private final String mDbUrl;
  private final Connection mConn;

  public UserTableInserter(String dbUrl) throws InstantiationException, IllegalAccessException,
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

  private final String insertQuery = "INSERT INTO User(id_original_usr, name_usr, "
      + "yelping_since_usr, review_count_usr, fans_usr, type_usr, avg_stars_usr, votes_funny_usr, "
      + "votes_useful_usr, votes_cool_usr, compliments_profile_usr, compliments_cute_usr, "
      + "compliments_funny, compliments_plain_usr, compliments_writer_usr, compliments_note_usr, "
      + "compliments_photos_usr, compliments_hot_usr, compliments_cool_usr, compliments_more_usr) "
      + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private final DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM");

  public void addToBatch(JSONObject jsonOb) throws SQLException, JSONException, ParseException {
    try (PreparedStatement prpdStmt = mConn.prepareStatement(insertQuery)) {
      prpdStmt.setString(1, jsonOb.getString("user_id"));
      prpdStmt.setString(2, jsonOb.getString("name"));
      prpdStmt.setDate(3,
          new Date(dateFormatter.parse(jsonOb.getString("yelping_since")).getTime()));
      prpdStmt.setInt(4, jsonOb.getInt("review_count"));
      prpdStmt.setInt(5, jsonOb.getInt("fans"));
      prpdStmt.setString(6, jsonOb.getString("type"));
      prpdStmt.setDouble(7, jsonOb.getDouble("average_stars"));
      
      prpdStmt.setInt(8, jsonOb.getInt("votes_funny_usr"));
    }
  }

  public static void main(String[] args) throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, SQLException, FileNotFoundException, IOException {
    String dbUrl = args[0];
    String inFilename = args[1];

    try (BufferedReader br = new BufferedReader(new FileReader(inFilename));
        UserTableInserter inserter = new UserTableInserter(dbUrl)) {
      inserter.mConn.setAutoCommit(false);
      // Each line is in json format (e.g., resources/user_sample.json)
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        JSONObject jsonOb = new JSONObject(line);
      }
    }
  }
}
