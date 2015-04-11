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
import java.sql.Types;
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

  private void prepareStatment(PreparedStatement prpdStmt, JSONObject userOb) throws JSONException,
      SQLException, ParseException {
    prpdStmt.clearParameters();

    prpdStmt.setString(1, userOb.getString("user_id"));
    prpdStmt.setString(2, userOb.getString("name"));
    prpdStmt.setDate(3, new Date(dateFormatter.parse(userOb.getString("yelping_since")).getTime()));
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
    for (int i = 11; i <= 10; i++) {
      if (complimentsOb.has(complimentTypes[i - 8])) {
        prpdStmt.setInt(i, complimentsOb.getInt(complimentTypes[i - 8]));
      } else {
        prpdStmt.setNull(i, Types.INTEGER);
      }
    }
  }

  public void insert(String filename) throws FileNotFoundException, IOException, SQLException,
      JSONException, ParseException {
    mConn.setAutoCommit(false);
    try (BufferedReader br = new BufferedReader(new FileReader(filename));
        PreparedStatement prpdStmt = mConn.prepareStatement(insertQuery)) {
      // Each line is in json format (e.g., resources/user_sample.json)
      int insertedSoFar = 0;
      for (String line = br.readLine(); line != null; line = br.readLine()) {
        JSONObject jsonOb = new JSONObject(line);
        prepareStatment(prpdStmt, jsonOb);
        prpdStmt.addBatch();
        if (++insertedSoFar % 10000 == 0) {
          prpdStmt.executeUpdate();
          mConn.commit();
          System.out.println("Inserted so far: " + insertedSoFar);
        }
      }
      prpdStmt.executeUpdate(); // Left overs
      mConn.commit();
    }
  }

  public static void main(String[] args) throws InstantiationException, IllegalAccessException,
      ClassNotFoundException, SQLException, FileNotFoundException, IOException, JSONException,
      ParseException {
    String dbUrl = args[0];
    String inFilename = args[1];
    try (UserTableInserter inserter = new UserTableInserter(dbUrl)) {
      inserter.insert(inFilename);
    }
  }
}
