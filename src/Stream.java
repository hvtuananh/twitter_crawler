import java.io.IOException;


// import org.knallgrau.utils.textcat.TextCategorizer;

// import twitter4j.Twitter;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.GeoLocation;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
// import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.StallWarning;
import twitter4j.auth.BasicAuthorization;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

import java.util.Calendar;
import java.util.Vector;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.IllegalStateException;

class StringsToProcess { 

	private Vector<String> list;

	
	public StringsToProcess()
	{
		list = new Vector<String>();
	}
	
	public synchronized void add(String st)
	{
		list.add(st);
	    notifyAll();
	}

	public synchronized String pop()
	{	
		if (list.size() == 0)
		{
	        try {
	            // wait for Consumer to get value
	            wait();
	        } catch (InterruptedException e) {
	        }
		}

		String lastElement = list.lastElement();
		list.remove(list.size()-1);
		
		// notify Consumer that value has been set
	    // notifyAll();
	    return lastElement;
	}

}


class Storage extends Thread {
	
	private BufferedWriter writer;
	private String   path;
	private String   radical;
	private Calendar current_calendar;
	private String   current_filename;
	
	StringsToProcess zip_stack;
	
	public Storage(String path, String radical)
	{
		this.path             = path;
		this.radical          = radical;
		this.writer           = null;
		this.current_calendar = null;
		this.current_filename = null;
		this.zip_stack        = new StringsToProcess();
	}

	private Calendar getCurrentCalendar()
	{
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal;
	}

	
	private void updateWriter() throws IOException
	{
		if (writer != null)
		{
			writer.close();
		}
		
		if (current_filename != null)
		{
			zip_stack.add(current_filename);
		}
	
		current_calendar = getCurrentCalendar();

		String directory = String.format(
				"%s/data/%04d-%02d-%02d",
				path,
				current_calendar.get(Calendar.YEAR),
				current_calendar.get(Calendar.MONTH)+1,
				current_calendar.get(Calendar.DAY_OF_MONTH)); 

		(new File(directory)).mkdirs(); // make sure directories exist
		 
		current_filename = String.format(
				//"%s/%s_%04d-%02d-%02d_%02dh_%02dm.json",
				"%s/%s_%04d-%02d-%02d_%02dh.json",
				directory,
				this.radical,
				current_calendar.get(Calendar.YEAR),
				current_calendar.get(Calendar.MONTH)+1,
				current_calendar.get(Calendar.DAY_OF_MONTH),
				current_calendar.get(Calendar.HOUR_OF_DAY)//,
				//current_calendar.get(Calendar.MINUTE)
				);

		writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(current_filename), "UTF-8"));
	}
	
	public synchronized void store(String data) throws IOException
	{
		Calendar c = this.getCurrentCalendar();
		if (writer == null || c.compareTo(this.current_calendar) > 0)
		{
			updateWriter();
		}
		writer.write(data);
		writer.write("\n");
		writer.flush();
	}
	
	
	public void run()
	{
		System.out.println("Starting Zip Monitor");
		
		// zip stuff
		while (true)
		{
			String filename = zip_stack.pop();
			try {
				{
					String arr = String.format("tar cvfz %s.tar.gz %s", filename, filename);  
					Process proc = Runtime.getRuntime().exec(arr);  
					proc.waitFor();  
				}
				{
					String arr = String.format("rm  %s", filename);  
					Process proc = Runtime.getRuntime().exec(arr);  
					proc.waitFor();  
				}	
			}
			catch (Exception e)
			{
				System.out.println("Problem Compressing File");
			}
		}
	}
}



public class Stream {

	Storage storage;
	
	//DataObjectFactory.set
	
	public Stream()
	{
		// Twitter store = new TwitterFactory("/store").getInstance();
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setJSONStoreEnabled(true);
		Configuration config = cb.build();
		
		final TwitterStream twitterStream = new TwitterStreamFactory(config).getInstance();
		boolean b = twitterStream.getConfiguration().isJSONStoreEnabled();
		// System.out.println("store enabled: "+b);
		
		storage = new Storage(".","twitter_us");
		StatusListener listener = new StatusListener(){
        	public void onStatus(Status status) {
        		
//        		GeoLocation geo = status.getGeoLocation();
//        		if (geo != null)
//        		{
//        			// System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
//            		System.out.println("   lon, lat: " + geo.getLongitude() + " " + geo.getLatitude());
//        		}

        		
        		// status.
        		try {
            		GeoLocation geo = status.getGeoLocation();
            		if (geo != null)
            		{
        			// System.out.print(status.getGeoLocation());
            			String rawJSON = DataObjectFactory.getRawJSON(status);
            			System.out.println(rawJSON);
            			storage.store(rawJSON);
            		}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw new RuntimeException("Problem when storing");
				}

//        		GeoLocation geo = status.getGeoLocation();
//        		if (geo != null)
//        		{
//        			System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
//            		System.out.println("   lon, lat: " + geo.getLongitude() + " " + geo.getLatitude());
//        		}
        		// long replyId = status.getInReplyToStatusId();
//        		if(replyId != -1 && !status.getSource().equals("web")){

        		
        		
        		
//        		if(!status.getSource().equals("web")){
//        			String tweet = status.getText();
//            		System.out.println(tweet);
//        			
//        			//String lang = guesser.categorize(Stream.cleanText(tweet));
//            		if(lang.equals("english") || lang.equals("irish")){
//            			System.out.println("ENGLISH>>>>" + tweet);
//            		}else{
//            			if(lang.equals("spanish") || lang.equals("catalan")){
//            				System.out.println("SPANISH>>>>" + tweet);
//            			}
//            		}
//            		
//        		}
        	}
        	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
        	public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
        	public void onException(Exception ex) {
        		ex.printStackTrace();
        	}
			@Override
			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub
			}
			public void onStallWarning(StallWarning warning){}
        };

    // TwitterStream twitterStream = new TwitterStreamFactory(listener).getInstance("lab_ara","q1uixad@");
    // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
        twitterStream.addListener(listener);

		FilterQuery locationFilter = new FilterQuery();
	    // double [][] brooklyn = {{-74.0780594, 40.5822408},{-73.8219406, 40.7176904}};
	    double [][] us = {
	    		{-125.72,25.4209},{-65.3962,49.6892}, // continental us
	    		{-170.0445, 52.6292},{-136.9694, 71.8265} // alaska
	    		};
	    locationFilter.locations(us);
	    System.out.println(locationFilter.toString());
	    
	    
	    
	    storage.start();
	    twitterStream.filter(locationFilter);
	}
	
//	public static final String cleanText(String tweet){
//		StringBuffer buffer = new StringBuffer();
//		boolean addedWord = false;
//		String[] words = tweet.split(" ");
//		for (int j = 0; j < words.length && words.length < 500 && words.length > 1; j++) {
//			if(words[j].startsWith("#") || words[j].startsWith("@") || words[j].contains("http://")){
//				continue;
//			}
//			if (!words[j].contains("http://")){
//				char[] charSentence = words[j].toCharArray();
//				int cons = 0;
//				boolean isCons = false;
//				boolean previousCons = false;
//				StringBuffer tempSentence = new StringBuffer();
//				for (int l = 0; l < charSentence.length && charSentence.length < 30; l++) {
//				   if(((int)charSentence[l] == 45 && charSentence.length > 1 && l != 0 && l != charSentence.length-1)  || ((int)charSentence[l] >= 48 && (int)charSentence[l] <= 57) ||
//					  ((int)charSentence[l] >= 65 && (int)charSentence[l] <= 90) ||
//					  ((int)charSentence[l] >= 97 && (int)charSentence[l] <= 122)){
//					  tempSentence.append(charSentence[l]);
//					  if(!((int)charSentence[l] == 65 || (int)charSentence[l] == 97
//						 || (int)charSentence[l] == 69 || (int)charSentence[l] == 101
//						 || (int)charSentence[l] == 73 || (int)charSentence[l] == 105
//						 || (int)charSentence[l] == 79 || (int)charSentence[l] == 111
//						 || (int)charSentence[l] == 85 || (int)charSentence[l] == 117)){
//						isCons = true;
//					  }else{
//			            isCons = false;
//			          }
//				   if(previousCons && isCons){
//					  cons++;
//			          if(cons > 4){
//			            break;
//			          }
//					}else{
//					  cons = 0;
//			        }
//			      }
//			      previousCons = isCons;
//			    }
//			    if(cons < 5){
//			    	buffer.append(tempSentence.toString().toLowerCase());
//			    	buffer.append(" ");
//			    	addedWord = true;
//				}
//				if(addedWord){
//			    	buffer.append(" ");
//				}
//			}
//		}
//		return buffer.toString();
//	}
	
	public static void main(String[] args) throws TwitterException, IOException{
	
        // Need to handle authentication first
	    // The factory instance is re-useable and thread safe.
        //Twitter twitter = TwitterFactory.getSingleton();
        Stream stream = new Stream();
		
		// final TextCategorizer guesser = new TextCategorizer();
		// guesser.setConfFile(args[0]);
	    // twitterStream.sample();
	}

	private static void storeAccessToken(long useId, AccessToken accessToken){
      //store accessToken.getToken()
      //store accessToken.getTokenSecret()
   }
}
