package  util;

import java.io.*;
import java.util.*;

public class Logger {
	
	public static final int INFO = 1;
	public static final int WARNING = 2;
	public static final int DEBUG = 3;
	public static final int VERBOSE = 3;
	
	String logFile;
	PrintWriter logWriter;
	int logLevel;
	boolean debug=false, info=false, warning=false, verbose=false;
	boolean printLevel = false;
	boolean printConsole = true;
	boolean printFile = true;
	boolean autoFlush = true;
	
	
	/**
	 * @param logfile file path where the log should be written
	 */
	/* **********************************************************
	 *
	 * **********************************************************/	
	public Logger(String logfile){
		this.logFile = logfile;
		try{
			logWriter = new PrintWriter(new FileOutputStream(logFile,true),true);
		}catch(IOException ioe){
			ioe.printStackTrace();
		}	
	}	
	
	public Logger(){
		printFile = false;
		printConsole = true;
	}
	
	public synchronized void logDebug(String msg){
		if(debug){
			log(msg,"DEBUG");
		}
	}
	
	public synchronized void logInfo(String msg){
		if(info){
			log(msg,"INFO");
		}
	}
	
	public synchronized void logWarning(String msg){
		if(warning){
			log(msg,"WARNING");
		}
	}
	
	public synchronized void logFatal(String msg){
		if(warning){
			log(msg,"FATAL_ERROR");
		}
	}
	
	public synchronized void logVerbose(String msg){
		if(verbose){
			log(msg,"VERBOSE");
		}
	}

	public synchronized void log(String msg,String level){
		String logLine = new Date().toString() +"|"+System.currentTimeMillis()+"|"+ level +"|"+  msg;
		if(printConsole)
			System.out.println(logLine);
		
		if(printFile)			
			logWriter.println(logLine);
		
	}
	
	public synchronized void logMsgOnly(String msg){
		if(printConsole){
			System.out.println(msg);
		}
		if(printFile)
			logWriter.println(msg);
	}
	
	public void setDebug(boolean debug) {
		this.debug = debug;
		System.out.println("Debug level set to "+debug);
	}
	
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
		System.out.println("Verbose level set to "+debug);
	}

	public void setInfo(boolean info) {
		this.info = info;
		System.out.println("Info level set to"+info);
	}

	public void setWarning(boolean warning) {
		this.warning = warning;
		System.out.println("Warning level set to"+warning);
	}
		

	public void setPrintConsole(boolean printConsole) {
		this.printConsole = printConsole;
	}
	
	public synchronized void flush(){
		if(printFile)
			logWriter.flush();
	}
	
}
