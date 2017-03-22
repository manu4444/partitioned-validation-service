package certifier;

import java.net.*;
import java.io.*;
import java.util.*;

import util.*;
import xact.StatsManager;



public class CertifierService {

	ServerSocket serverSocket;
	DependencyManager dsgManager;
	
	public CertifierService(int port) throws IOException{
		serverSocket = new ServerSocket(port);
		dsgManager = new DependencyManager();
	}
	
	public void run(){
		try {
			while(true){		
				Socket clientSocket = serverSocket.accept();				
				CertifierRequestHandler handler = new CertifierRequestHandler(clientSocket,dsgManager);
				handler.start();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch(Exception e){
			e.printStackTrace();
		}			
		finally{
			try{
				serverSocket.close();
			}catch(Exception e){}
		}
		
	}
	
	public static void main(String args[]) throws IOException{
		String configFile = args[0];
		Config config = new Config(configFile);		
		int port = config.getIntValue("certifierServerPort");
		CertifierService certifier  = new CertifierService(port);										  
		System.out.println("Certifier service started...");
		LoggerThread loggerThread = new LoggerThread();
		loggerThread.start();
		certifier.run();		
	}	
	
}

class LoggerThread extends Thread {		
	public void run() {
		while(true){
			try {
				Thread.sleep(60*1000);
				System.out.println(new Date());
				System.out.println("Total Commits:"+CertifierRequestHandler.totalCmt);
				System.out.println("Total Aborts:"+CertifierRequestHandler.totalAbort);
				System.out.println("Total SI Aborts:"+CertifierRequestHandler.totalSIAbort);
				System.out.println("Total Ser Aborts:"+CertifierRequestHandler.totalSerAbort);
				System.out.println();
			}catch(Exception e){}
		}
	}
}
