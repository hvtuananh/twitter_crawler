/*
 * @author Tuan-Anh Hoang-Vu, Lauro Lins
 *
 */
import java.lang.IllegalStateException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Vector;
import java.util.Properties;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.GeoLocation;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.StallWarning;
import twitter4j.auth.BasicAuthorization;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

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
            "%s/%s_%04d-%02d-%02d_%02dh.json",
            directory,
            this.radical,
            current_calendar.get(Calendar.YEAR),
            current_calendar.get(Calendar.MONTH)+1,
            current_calendar.get(Calendar.DAY_OF_MONTH),
            current_calendar.get(Calendar.HOUR_OF_DAY)
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
    
    public Stream()
    {
        Properties prop = new Properties();
        String propFileName = "config.properties";
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
        try {
            prop.load(inputStream);
        }
        catch (Exception e){
            System.out.println("Configuration File Does Not Exists");
        }
        if (inputStream == null) {
            System.out.println("Cannot Read Configuration File");
        }

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setJSONStoreEnabled(true);
        Configuration config = cb.build();
        
        final TwitterStream twitterStream = new TwitterStreamFactory(config).getInstance();
        boolean b = twitterStream.getConfiguration().isJSONStoreEnabled();
        
        storage = new Storage(".","twitter");
        StatusListener listener = new StatusListener(){
            public void onStatus(Status status) {
                // status.
                try {
                    String rawJSON = DataObjectFactory.getRawJSON(status);
                    System.out.println(rawJSON);
                    storage.store(rawJSON);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    throw new RuntimeException("Problem when storing");
                }
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

        twitterStream.addListener(listener);

        String type = prop.getProperty("type");
        switch (type) {
            case "filter":
                FilterQuery filter = new FilterQuery();
                filter.track(prop.getProperty("keywords").split(","));
                break;
            case "geofilter":
                // not yet implemented
                break;
            case "sample":
                storage.start();
                twitterStream.sample();
                break;
            default:
                break;
        }

        // FilterQuery locationFilter = new FilterQuery();
        // double [][] us = {
        //         {-125.72,25.4209},{-65.3962,49.6892}, // continental us
        //         {-170.0445, 52.6292},{-136.9694, 71.8265} // alaska
        //         };
        // locationFilter.locations(us);
        // System.out.println(locationFilter.toString());
        
        // storage.start();
        // twitterStream.filter(locationFilter);
    }
    
    public static void main(String[] args) throws TwitterException, IOException{
    
        // Need to handle authentication first
        // The factory instance is re-useable and thread safe.
        Stream stream = new Stream();
    }
}
