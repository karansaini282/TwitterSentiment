import java.util.Properties;
import java.util.Scanner; 
import java.util.List;
import java.io.*;

import twitter4j.*;
import twitter4j.conf.*;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TwitterSource implements Runnable{
	
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;
    private Twitter twitter;
    private String searchQuery;
    private long sinceId;
    private long maxId;
    
    public TwitterSource()
    {
    	this.sinceId=0;
    }
    
    public TwitterSource(long sinceId)
    {
    	this.sinceId=sinceId;
    }
    
    public void run(){
	Properties prop = new Properties();
	InputStream input = null;

	try {

        Properties configProperties = new Properties();
        configProperties.put("bootstrap.servers","localhost:9092");
        configProperties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(configProperties);
		
		input = getClass().getClassLoader().getResourceAsStream("twitter.properties");

		// load a properties file
		prop.load(input);

		// get the property value and print it out
        consumerKey=prop.getProperty("consumerKey");
	consumerSecret=prop.getProperty("consumerSecret");
	accessToken=prop.getProperty("accessToken");
        accessTokenSecret=prop.getProperty("accessTokenSecret");
        searchQuery=prop.getProperty("searchQuery");
        

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
        cb.setOAuthAccessToken(accessToken);
        cb.setOAuthAccessTokenSecret(accessTokenSecret);
        cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(true);
 
        twitter = new TwitterFactory(cb.build()).getInstance();

        //List<String> items = Arrays.asList(str.split("\\s*,\\s*"));
       
        try {
            Query query = new Query(searchQuery);
         	query.setLang("en");
         	if(sinceId!=0)
         	{
         		query.setSinceId(sinceId);
         	}         	
            QueryResult result;
            result = twitter.search(query);
            this.maxId=result.getMaxId();
            List<Status> tweets = result.getTweets();
            for (Status tweet : tweets) {
                System.out.println("Id: " + tweet.getId() + " @" + tweet.getUser().getScreenName() + " - " + tweet.getText());
                ProducerRecord<String, String> rec = new ProducerRecord<String, String>("brandData",tweet.getText());
                producer.send(rec);
            }
            producer.close();
            //System.exit(0);
        } catch (TwitterException te) {
            te.printStackTrace();
            System.out.println("Failed to search tweets: " + te.getMessage());
            System.exit(-1);
        }



	} catch (IOException ex) {
		ex.printStackTrace();
	} finally {
		if (input != null) {
			try {
				input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

  }
   
   public long getMaxId()
   {
	   return this.maxId;
   }
    
   public static void main(String[] args) {
          Scanner scan = new Scanner(System.in);
          String msg;
          long maxId=0;
	      try {
	    	 do{ 
	         TwitterSource obj = new TwitterSource(maxId);
	         Thread t=new Thread(obj);
	         t.start();
	         t.join();
	         maxId=obj.getMaxId();
	         System.out.println("MaxId: "+String.valueOf(maxId));
	         System.out.println("Continue: ");
             msg = scan.next();
	    	 }while(!msg.equals("No"));
	    	 scan.close();
	      }catch(Exception e) {
	         e.printStackTrace();
	      }
	   
    }
} 
