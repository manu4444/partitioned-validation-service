package xact;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;


public class TimestampServer {  

	public static final int APPL_PORT = 30000;
	ServerSocket serverSocket = null;
	
	String hostName = "";
	int updateNumber = 0;

	long timestamp = 0;	
	long sts = 0;
	

	int tid = 0;
	
	LinkedList<Long> uncmtTS;
	Hashtable<Integer,Boolean> issuedTids;
	Random rndSource;
	ThreadPoolExecutor pool;
	BlockingQueue<Runnable> workQueue;
	
	public TimestampServer(int applPort)
        throws IOException {    
		serverSocket = new ServerSocket(applPort);
		hostName = InetAddress.getLocalHost().getCanonicalHostName();
		System.out.println("TimestampServer started at port:"+applPort);
		uncmtTS = new LinkedList<Long>();
		issuedTids = new Hashtable<Integer, Boolean>();
		rndSource = new Random();
		workQueue = new LinkedBlockingQueue<Runnable>();
		pool = new ThreadPoolExecutor(50, 50, 10, TimeUnit.MINUTES, workQueue);
	}	

	public int getPort(){
		return serverSocket.getLocalPort();
	}

	public String getHostName(){
		return hostName;
	}
	
	public void run(){
		try {
			StatusThread stThread = new StatusThread();
            stThread.start();
			while(true){		
				Socket clientSocket = serverSocket.accept();				
				//System.out.println("New request");				
				RequestHandler handler = new RequestHandler(clientSocket, this);
				pool.submit(handler);						
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
	
	public static void main(String args[]){
        try{        	
        	int applport = Integer.parseInt(args[0]);
        	TimestampServer server = new TimestampServer(applport);
        	for(int i=0; i<args.length; i++) {
        		String val =args[i].substring(args[i].indexOf('=')+1);
        		if(args[i].startsWith("-sts")) {
        			server.sts = Long.parseLong(val);
        		} else if(args[i].startsWith("-ts")) {
        			server.timestamp = Long.parseLong(val);
        		}
        	}                        
            server.run();            

        }catch(IOException e){
            System.out.println("IO Exception in starting  TimestampServer");
            e.printStackTrace();
        }    	
	}
		
	public synchronized long getTS(){		
		timestamp = timestamp+1;		
		uncmtTS.add(new Long(timestamp));
		//System.out.println("issued ts "+timestamp);
		return timestamp;
	}
	
	public synchronized int getTID(){
		/*
		int tid = rndSource.nextInt(Integer.MAX_VALUE);
		while(issuedTids.containsKey(tid)){
			tid = rndSource.nextInt(Integer.MAX_VALUE);
		}
		issuedTids.put(tid,true);
		return tid;
		*/
		tid++;
		return tid;
	}
	
	public long getSts() {
		return sts;
	}
	
	public synchronized void checkForGap() {
		if(uncmtTS.size() > 0 ){
			long first = uncmtTS.getFirst();					
			while(timestamp-first > 500) {
				//right now just assume lagging transaction has aborted and breach the gap.
				uncmtTS.removeFirst();				
				if(!uncmtTS.isEmpty()) {
					first = uncmtTS.getFirst();
					System.out.println("breaching gap from "+sts+" to "+first);
					sts = first-1;					
				}
				else{
					System.out.println("breaching gap from "+sts+" to "+timestamp);
					sts = timestamp;
					first = timestamp+1;
				}				
			}
		}
	}
	
	public synchronized void updateSTS(long newSTS){		
		uncmtTS.remove(new Long(newSTS));				
		if(newSTS == sts+1){
			sts = newSTS;
		}
		if(uncmtTS.size() > 0 ){
			long first = uncmtTS.getFirst();
			if(first-1 > sts)
				sts = first-1;			
		}else{
			//there is no gap, all transactions are committed 
			sts = timestamp;
		}		
		
		//System.out.println("updated STS to "+sts);	
	}
	
	class RequestHandler implements Runnable {
		Socket clientSocket;		
		TimestampServer server ;
		RequestHandler(Socket clientSocket, TimestampServer server){
			this.clientSocket = clientSocket;			
			this.server = server;
		}
		
		public void run() {
			//System.out.println("Serving request");
			try{
				DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
				int reqType = dis.readInt();
				if(reqType==1) { //getSTS
					//System.out.println("Request getSTS");
					DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
					long curSTS = server.sts;
					dos.writeLong(curSTS);
					int tid = server.getTID();
					dos.writeInt(tid);
					dos.close();
				} else if(reqType == 2){ //updateSTS
					long newSTS = dis.readLong();
					server.updateSTS(newSTS);
				}
				else if (reqType == 3) { //getTS
					DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
					long ts = server.getTS();					
					dos.writeLong(ts);
					dos.close();
				}
				dis.close();
				clientSocket.close();
				//System.out.println("Completed request");
				// checkForGap();
			}catch(IOException ioe){
				System.out.println("Error in handling timestamp request");
				ioe.printStackTrace();
			}
		}
		
		
	}
	
	class StatusThread extends Thread {
		public void run() {
			while(true){
				try{
					Thread.sleep(10000);
					System.out.print("STS: "+sts+" GTS:"+timestamp);
					if(uncmtTS.size()>0) 
						System.out.print(" First Uncommitted:"+uncmtTS.getFirst());
					if(uncmtTS.size()>1)
						System.out.print(" Second Uncommitted:"+uncmtTS.get(1));
					
					System.out.println();
				}catch(Exception e){}
			}
		}
	}

}




