package replicatedCertifier;

import java.util.*;
import java.io.*;
import java.net.*;
import java.rmi.*;

public class ReplicationConfig {
	HashMap<String, PeerInterface> peerHandleTable;
	HashMap<Integer, String> indexTable;
	String myHostName;
	int myID;
	
	public ReplicationConfig() {
		peerHandleTable = new HashMap<String, PeerInterface>();
		indexTable = new HashMap<Integer, String>();
	}
	
	public ReplicationConfig(String configFile, PeerInterface localHandle) {
		peerHandleTable = new HashMap<String, PeerInterface>();
		indexTable = new HashMap<Integer, String>();
		try {
		      BufferedReader bufferedReader = new BufferedReader(new FileReader(configFile));
		      String host;
		      int i=0;
		      while ((host = bufferedReader.readLine()) != null) {
		    	  String url  = "//" + host  + "/PeerInterface";		           
		           indexTable.put(i, host);
		            String fullHostName = InetAddress.getLocalHost().getCanonicalHostName();		            
					if (fullHostName.equals(host)) {
						System.out.println("my server:"+fullHostName);
		            	peerHandleTable.put(host, localHandle);
		            	myHostName = host;
		            	myID = i;
		            } else {
		            	PeerInterface peerHandle = lookup(url);		            	
		            	peerHandleTable.put(host, peerHandle);
		            }
					i++;
		      }
		}catch(Exception e) {
			e.printStackTrace();
		}
		System.out.println("My config:");
		System.out.println(indexTable);
		System.out.println(peerHandleTable);
	}
	
	private PeerInterface lookup(String url) {
		System.out.println("Looking up peer:"+url);
		while(true) {
			try {
				PeerInterface peerHandle = (PeerInterface)Naming.lookup(url);
				System.out.println("Peer up");
				return peerHandle;
			} catch(Exception e){
				try { Thread.sleep(10); } catch(Exception e1){}
			}
		}
	}
	
	public PeerInterface getHandle(String host) {
		return peerHandleTable.get(host);
	}
	
	public void addHandle(String host, PeerInterface peerHandle) {
		indexTable.put(indexTable.size(), host);
		peerHandleTable.put(host, peerHandle);
	}
	
	public String getServerForItem(long itemID) {
		int idx = (int) (itemID % indexTable.size());
		String host = indexTable.get(idx);
		return host;
	}
	
}
