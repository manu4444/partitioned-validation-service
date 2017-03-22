package replicatedCertifier;

import java.util.*;
import java.rmi.*;
import java.net.*;
import java.io.*;
import util.*; 

public class ScalabilityTest {	
	public static void main(String args[]) {
		String configFile = args[0];
		Vector<CertifierInterface> replicaList = new Vector<CertifierInterface>();
		try {
		      BufferedReader bufferedReader = new BufferedReader(new FileReader(configFile));
		      String host;
		      int i=0;
		      while ((host = bufferedReader.readLine()) != null) {
		    	  String url  = "//" + host  + "/PeerInterface";       
		          CertifierInterface replica = (CertifierInterface) Naming.lookup(url);     
		          replicaList.add(replica);
		      }
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		int numThreads = Integer.parseInt(args[1]);
		int id = Integer.parseInt(args[2]);
		TestClient.counter = id * 1000000;
		TestClient[] clients = new TestClient[numThreads];
		for (int i=0; i<numThreads; i++) {
			TestClient client = new TestClient();
			client.replicaList = replicaList;
			clients[i] = client; 
			client.start();			
		}
		Printer printer = new Printer();
		printer.clients = clients;
		printer.start();
	}
}

class TestClient extends Thread {
	Vector<CertifierInterface> replicaList;
	public static Random rnd;
	static {
		rnd = new Random();
	}
	static int counter=0;
	static Object lock = new Object();
	static long committed, aborted;
	int total=0;
	long sum=0;
	int next=0;
	public static final long ITEM_COUNT = 1000000;
	public static final int RS_SIZE = 4;
	public static final int WS_SIZE = 8;
	
	public void run() {
		while(true) {
			try {
				doTxn();				
			} catch (Exception e){
				//e.printStackTrace();
			}
		}
	}
	
	public static int getTID() {
		synchronized(lock) {
			counter++;
			return counter;
		}
	}
	public void doTxn() throws Exception {
		/*
		String host = "nuclear12.cs.umn.edu";
		int port = 30000;		
		Socket socket = new Socket(host, port);
		DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
		dos.writeInt(1);
		DataInputStream dis = new DataInputStream(socket.getInputStream());
		long sts = dis.readLong();
		int tid = dis.readInt();
		dis.close();
		dos.close();
		socket.close();
		*/
		 
		List<Long> wset = new Vector<Long>();
		List<Long> rset = new Vector<Long>();
		for (int i=0; i < RS_SIZE; i++) {
			long key = rnd.nextLong() % ITEM_COUNT;
			if (key < 0) 
				key = key*-1;
			rset.add(key);
		}
		for (int i=0; i < WS_SIZE; i++) {
			long key = rnd.nextLong() % ITEM_COUNT;
			if (key < 0) 
				key = key*-1;
			wset.add(key);
		}		
		int tid = getTID();
		RWSet rwSet = new RWSet(Long.MAX_VALUE, tid, wset, rset);
		HashMap<String, RWSet> rwSetMap = new HashMap<String, RWSet>();
		rwSetMap.put("TestTable", rwSet);
		int idx = (next++) % replicaList.size();
		CertifierInterface certifier = replicaList.get(idx);
		long start = System.currentTimeMillis();
		long commitTS = certifier.certify(tid, rwSetMap);
		long end = System.currentTimeMillis();
		long latency = end-start;
		total++;
		sum = sum+latency;
	}	
}

class Printer extends Thread {
	TestClient[] clients;
	public void run() {
		while(true) {
		try {
			Thread.sleep(1000);
			int total=0;
			long sum=0;
			for (int i=0; i<clients.length; i++) {
				total += clients[i].total;
				sum += clients[i].sum;
				clients[i].total =0;
				clients[i].sum =0;
			}
			float avg = (float)sum/total;
			System.out.println(new Date());
			System.out.println("Total: "+total+" Avg Latency: "+avg);			
			System.out.println();
		} catch(Exception e){
			e.printStackTrace();			
		}
		}
	}
}
