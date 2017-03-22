package util;

import java.util.*;
import java.io.*;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.conf.*;


public class Config {
	protected static Config config = null;
	
    private String configFile = null ;
    Hashtable<String,Object> configTable;
    Logger logger = null;
    Configuration hbaseConfig = null;

    public Config(){
        configTable = new Hashtable<String,Object>();
        createLogger();
        hbaseConfig = HBaseConfiguration.create();
        config = this;
    }

    public Config(String configFile){
        this.configFile = configFile;
        configTable = new Hashtable<String,Object>();
        readConfigFile();        
        createLogger();
        hbaseConfig = HBaseConfiguration.create();
        config = this;
    }

    private void readConfigFile(){
        try{
            FileReader fr = new FileReader(configFile);
            BufferedReader bread = new BufferedReader(fr);
            String line;
            while (bread.ready()) {
                line = bread.readLine();
                if(line.charAt(0)=='#')
                    continue;
                System.out.println(line);
                StringTokenizer st = new StringTokenizer(line, "=");
                String key = st.nextToken();
                String value = st.nextToken();
                configTable.put(key, value);
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    private void createLogger(){    	
    	if(has("logFile")){
    		String logfile = getStringValue("logFile");
    		logger = new Logger(logfile);
    	}
        else{
           	logger = new Logger();
        }
        String loglevels = getStringValue("logLevels");
        if (loglevels.contains("debug"))
        	logger.setDebug(true);
        if (loglevels.contains("info"))
        	logger.setInfo(true);
        if (loglevels.contains("warning"))
        	logger.setWarning(true);
        if (loglevels.contains("verbose"))
        	logger.setVerbose(true);
        
        //if(getBoolValue("printConsole")==false)
        	//logger.setPrintConsole(false);
    }
    
    public Object get(String key){
        return configTable.get(key);
    }

    public String getStringValue(String key){
        return (String)get(key);
    }

    public boolean getBoolValue(String key){
        Boolean boolVal = new Boolean((String)get(key));
        return boolVal.booleanValue();
    }

    public int getIntValue(String key){
        Integer intVal = new Integer((String)get(key));
        return intVal.intValue();
    }

    public long getLongValue(String key){
        Long longVal = new Long((String)get(key));
        return longVal.longValue();
    }
    
    public boolean has(String key){
        return configTable.containsKey(key);
    }

    public void add(String key,Object value){
        configTable.put(key, value);
    }
    
    public Logger getLogger(){
    	return logger;
    }
    
    public Configuration getHBaseConfig(){
    	return hbaseConfig;
    }
    
    public static Config getInstance(){
    	return config;
    }
}