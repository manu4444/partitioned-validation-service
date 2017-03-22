package replicatedCertifier;

import java.net.*;
import java.io.*;
import java.rmi.*;
import java.util.*;

import util.*;
import xact.TimestampServer;



public class CorrectnessTest extends Thread {
	public static final int xVecSize = 4;
	public static final int ITEM_COUNT = 1000;

	public Random rnd;
	public Random rndVal;
	CertifierInterface certifier;	
	Hashtable<Long, Integer> dataTable;
	static int committed, aborted;
	TimestampServer ts;
	
	static int thrCount = 0;
	
	public void run() {
		for (int i=0; i<100; i++) {
			try {
				doTxn();
			} catch(Exception e){
				e.printStackTrace();
			}
		}
		thrCount--;
		if (thrCount==0) {
			System.out.println("Committed:"+committed+" Aborted:"+aborted);
			for (long i=0 ; i<ITEM_COUNT; i++) {
				long x1 = i*6;
				long x2 = x1+1;
				long x3 = x2+1;
				long x4 = x3+1;
				long y = x4+1;
				long z = y+1;
				
				// System.out.println(x1);
				int x1_val = dataTable.get(x1);
				int x2_val = dataTable.get(x2);
				int x3_val = dataTable.get(x3);
				int x4_val = dataTable.get(x4);
				int y_val = dataTable.get(y);
				int z_val = dataTable.get(z);
				
				if (z_val != (x1_val + x2_val + x3_val+ x4_val + y_val)) {
					System.out.println("Constraint violation:");
					System.out.println(x1_val +" "+x2_val+" "+x3_val+" "+x4_val+" "+y_val+" "+z_val);
				}
				// System.out.println(dataTable);
				
			}
			System.out.println(System.currentTimeMillis());
		}
	}

	public void doTxn() throws Exception {
		//System.out.println("Starting txn");
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
		
		long sts = ts.getSts();
		int tid = ts.getTID();
		long x1 = rnd.nextInt(ITEM_COUNT) * 6;
		long x2 = x1+1;
		long x3 = x2+1;
		long x4 = x3+1;
		long y = x4+1;
		long z = y+1;
		
		// System.out.println(x1);
		int x1_val = dataTable.get(x1);
		int x2_val = dataTable.get(x2);
		int x3_val = dataTable.get(x3);
		int x4_val = dataTable.get(x4);
		
		int y_val = rndVal.nextInt(5000);
		int z_val = x1_val + x2_val + x3_val + x4_val + y_val;
		
		List<Long> rset = new Vector<Long>();
		rset.add(x1); 
		rset.add(x2);
		rset.add(x3);
		rset.add(x4);
		List<Long> wset = new Vector<Long>();
		wset.add(y);
		wset.add(z);
		
		RWSet rwSet = new RWSet(sts, tid, wset, rset);
		HashMap<String, RWSet> rwSetMap = new HashMap<String, RWSet>();
		rwSetMap.put("DataTable", rwSet);
		long commitTS = certifier.certify(tid, rwSetMap);
		if (commitTS > 0) {
			committed++;
			commitTS = ts.getTS();
			dataTable.put(y, y_val);
			dataTable.put(z, z_val);
			ts.updateSTS(commitTS);
		}
		else
			aborted++;		
		//System.out.println("Completed txn");
	}

	public void advanceStableTS(long newSTS) throws IOException {
		// Put p = new Put(Bytes.toBytes(STS_CTR_ROWID));
		// p.add(Bytes.toBytes("d"),
		// Bytes.toBytes("val"),Bytes.toBytes(newSTS));
		// boolean done = counterTable.checkAndPut(Bytes.toBytes(STS_CTR_ROWID),
		// Bytes.toBytes("d"), Bytes.toBytes("val"),
		// Bytes.toBytes(newSTS-1), p);
		
		Socket socket = new Socket("nuclear12.cs.umn.edu", 30000);
		DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
		dos.writeInt(2);
		dos.writeLong(newSTS);
		dos.close();
		socket.close();
	}

	public static void main(String args[]) throws Exception {
		String host = args[0];
		String url = "//" + host+"/CertifierInterface";
		CertifierInterface certifier = (CertifierInterface) Naming.lookup(url);
		Hashtable<Long, Integer> dataTable = new Hashtable<Long, Integer>();
		for (long i=0; i<(ITEM_COUNT); i++) {
			long x1 = i*6;
			long x2 = x1+1;
			long x3 = x2+1;
			long x4 = x3+1;
			long y = x4+1;
			long z = y+1;
			int x1_val = (int) (Math.random()*1000);
			int x2_val = (int) (Math.random()*2000);
			int x3_val = (int) (Math.random()*3000);
			int x4_val = (int) (Math.random()*4000);
			int y_val = (int) (Math.random()*5000);
			int z_val = x1_val + x2_val + x3_val + x4_val + y_val;
			
			dataTable.put(x1, x1_val);
			dataTable.put(x2, x2_val);
			dataTable.put(x3, x3_val);
			dataTable.put(x4, x4_val);
			dataTable.put(y, y_val);
			dataTable.put(z, z_val);
		}
		Random rnd = new Random(1234);
		Random rndVal = new Random(99090);
		CorrectnessTest.thrCount = 100;
		System.out.println(System.currentTimeMillis());
		
		TimestampServer ts = new TimestampServer(30000);
		
		for (int i=0; i<100; i++) {
			CorrectnessTest test = new CorrectnessTest();
			test.certifier = certifier;
			test.dataTable = dataTable;
			test.rnd = rnd;
			test.rndVal = rndVal;
			test.ts = ts;
			test.start();
		}
		
		
	}

	
}
	
	


