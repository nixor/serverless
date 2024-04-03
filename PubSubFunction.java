package gcfv2pubsub;
 
import com.google.cloud.functions.CloudEventsFunction;
import com.google.events.cloud.pubsub.v1.MessagePublishedData;
import com.google.events.cloud.pubsub.v1.Message;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cloudevents.CloudEvent;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.sql.*;
import java.util.Base64;
import java.util.logging.Logger;
 
public class PubSubFunction implements CloudEventsFunction {
 
  private static final Logger logger = Logger.getLogger(PubSubFunction.class.getName());
  private final String mailgunApiKey = System.getenv("api_key");
  private final String mailgunDomainName = "nixor.me";
  private final String verificationBaseUrl = "https://nixor.me/v1/verify/";
  private final String dblink = System.getenv("dbUrl");
  private final String dbname = System.getenv("dbName");
  private final String dbPass = System.getenv("dbPass");// Base URL for verification endpoint
 
 
  @Override
  public void accept(CloudEvent event)  {
    // Get cloud event data as JSON string
    String cloudEventData = new String(event.getData().toBytes());
    // Decode JSON event data to the Pub/Sub MessagePublishedData type
    Gson gson = new Gson();
    MessagePublishedData data = gson.fromJson(cloudEventData, MessagePublishedData.class);
    // Get the message from the data
    Message message = data.getMessage();
    // Get the base64-encoded data from the message & decode it
    String encodedData = message.getData();
    String decodedData = new String(Base64.getDecoder().decode(encodedData));
    // Log the message
    logger.info("Pub/Sub message: " + decodedData);
    JsonObject jsonObject = JsonParser.parseString(decodedData).getAsJsonObject();
    String email = jsonObject.get("email").getAsString();
    String id = jsonObject.get("id").getAsString(); // Assuming UUID is provided in the message
    logger.info("Trimmed Email: " + email);
    logger.info("UUID: " + id);
    String verificationLink = verificationBaseUrl + id; // Use id in the verification link
    String emailBody = "This is a test email to verify sending functionality. Please click on the following link to verify your email address: " + verificationLink;
    testEmailSending(email, emailBody);
    saveToDatabase(email, System.currentTimeMillis()); // Pass current time as email sending time
 
  }
  private void testEmailSending(String email, String emailBody) {
    try {
      String url = "https://api.mailgun.net/v3/" + mailgunDomainName + "/messages";
      HttpResponse<JsonNode> request = Unirest.post(url)
              .basicAuth("api", mailgunApiKey)
              .field("from", "nikhil@nixor.me") // Replace with your sender email
              .field("to", email)
              .field("subject", "Hello from Pub/Sub Function")
              .field("text", emailBody)
              .asJson();
      if (request.getStatus() != 200) {
        throw new UnirestException("Failed to send email: " + request.getBody());
      } else {
        logger.info("Email sent successfully to " + email);
      }
    } catch (UnirestException e) {
      logger.severe("Error sending email: " + e.getMessage());
    }
  }
 
 
  private void saveToDatabase(String email,  long sent_time) {
    String dbUrl = dblink;
    String dbUsername = dbname;
    String dbPassword = dbPass;
    logger.info("dbUrl: " + dbUrl);
    logger.info("dbUsername: " + dbUsername);
    logger.info("dbPassword: " + dbPassword);
    try {
      // Connect to the database
      Connection conn = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
      String createTableSql = "CREATE TABLE IF NOT EXISTS email_schedule (email VARCHAR(250), sent_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";
      conn.createStatement().execute(createTableSql);
      logger.info("Connected to the database.");
      String insertSql = "INSERT INTO email_schedule (email) VALUES (?)";
      PreparedStatement preparedStatement = conn.prepareStatement(insertSql);
      preparedStatement.setString(1, email); // Example verification_expiration value
      int rowsAffected = preparedStatement.executeUpdate();
      logger.info("Rows affected by insertion: " + rowsAffected);
      preparedStatement.close();
      conn.close();
    } catch (SQLException e) {
      logger.info("Error inserting data into the database: " + e.getMessage());
    }
 
  }
 
}