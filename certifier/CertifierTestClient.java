package certifier;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.hbase.util.*;


import xact.*;
import util.*;


public class CertifierTestClient extends Thread{


	public CertifierTestClient() {
		
	}

	public void run() {
		while(true) {
			getCertification();
		}
	}
	
	/**
	 * @param args
	 */
		private void getCertification() {
			Random rnd = new Random();
        	Hashtable<String, Pair<Collection<byte[]>, Collection<byte[]>>> rwSet=
            new Hashtable<String, Pair<Collection<byte[]>, Collection<byte[]>>>();
        	Vector<byte[] > readSet = new Vector<byte[]>();
        	Vector<byte[] > writeSet= new Vector<byte[]>();
        	for (int i=0; i<10;i++){       	
            	byte bytes[] = new byte[64];
				rnd.nextBytes(bytes);
				readSet.add(bytes);
            }
            for(int i=0; i<10;i++){
            	byte bytes[] = new byte[64];
				rnd.nextBytes(bytes);
                writeSet.add(bytes);
            }
			
            Pair<Collection<byte[]>, Collection<byte[]>> rw = new Pair<Collection<byte[]>, Collection<byte[]>>();
            rw.setFirst(readSet);
            rw.setSecond(writeSet);
            rwSet.put("table1", rw);
        	
			long snapshotTS = 0;
			long commitTS = rnd.nextLong();
			int tid = rnd.nextInt();	
        	CertifierRequest request = new CertifierRequest(CertifierRequest.CERTIFY_BOTH, rwSet,snapshotTS,commitTS,tid);
        	try{
        	    String host = Config.getInstance().getStringValue("certifierServerName");
           		int port = Config.getInstance().getIntValue("certifierServerPort");
            	Socket socket = new Socket(host,port);
            	ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            	out.writeObject(request);
           		ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            	int status = in.readInt();            	
       		}catch(IOException ioe){
            	System.out.println("<TransactionHandler#T"+tid+"> Error getting certification:"+ioe.getMessage());
        	}
    	}
					
	
	public static void main(String args[]) {
		Config config = new Config(args[0]);	
		for(int i=0; i<20;i++) {
			CertifierTestClient client = new CertifierTestClient();
			client.start();
		}	
	}
}
