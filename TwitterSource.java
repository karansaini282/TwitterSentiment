import java.util.Properties;
import java.io.*;
import java.util.concurrent.LinkedBlockingQueue;

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
    private String searchQuery;
    
    public void run(){
    
    try{
	Properties prop = new Properties();
	InputStream input = null;

		LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);

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
        input.close();

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey(consumerKey);
        cb.setOAuthConsumerSecret(consumerSecret);
        cb.setOAuthAccessToken(accessToken);
        cb.setOAuthAccessTokenSecret(accessTokenSecret);
        cb.setJSONStoreEnabled(true);
        cb.setIncludeEntitiesEnabled(true);
 
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        StatusListener listener = new StatusListener() {
          
           @Override
           public void onStatus(Status status) {      
        	   queue.offer(status);
           }
           
           
           @Override
           public void onDeletionNotice(StatusDeletionNotice x) {
              //System.out.println("Got a status deletion notice id:" + x.getStatusId());
           }
           
           @Override
           public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
              // System.out.println("Got track limitation notice:" + num-berOfLimitedStatuses);
           }

           @Override
           public void onScrubGeo(long userId, long upToStatusId) {
              // System.out.println("Got scrub_geo event userId:" + userId + "upToStatusId:" + upToStatusId);
           }      
           
           @Override
           public void onStallWarning(StallWarning warning) {
              // System.out.println("Got stall warning:" + warning);
           }
           
           @Override
           public void onException(Exception ex) {
              ex.printStackTrace();
           }   
        
        };
        twitterStream.addListener(listener);
        
        FilterQuery query = new FilterQuery().track(searchQuery);
        twitterStream.filter(query);
        
        while(true) {
            Status ret = queue.poll();
            
            if (ret == null) {
               Thread.sleep(100);
            }else {             
                  System.out.println("@" + ret.getUser().getScreenName() +": " + ret.getText());
                  producer.send(new ProducerRecord<String, String>(
                     "brandData",ret.getText()));
            }
         }

    }
    catch(Exception e)
    {
    	e.printStackTrace();
    }
  }
    
   public static void main(String[] args) {
	      try {
	         TwitterSource obj = new TwitterSource();
	         Thread t=new Thread(obj);
	         t.start();
	         t.join();
	      }catch(Exception e) {
	         e.printStackTrace();
	      }
	   
    }
} 
