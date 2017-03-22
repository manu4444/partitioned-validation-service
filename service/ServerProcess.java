package service;

import java.util.*;
import java.io.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

import xact.*;
import util.*;

public class ServerProcess {
	TransactionManager xactManager;
	int pid = 0;
	
	public ServerProcess() throws IOException {
		xactManager = new TransactionManager();	
		String[] tableNames = {"table1"};
		Batcher.startNewBatcher(Arrays.asList(tableNames));		
	}
	
	public void run() {		
		//basicTest();
		//conflictTestSI();
		//serializabilityTest();
		concurrencyTest();	
		StatsManager.logStats();
	}
	
	private void conflictTestSI(){
		System.out.println("********Performing SI conflict test******");
		try{
			TransactionHandler t1 = xactManager.startTransaction();
			Get g = new Get(Bytes.toBytes("SIrow1"));
			Result r = t1.get("table1", g);
			System.out.println("Transaction#"+t1.getTid()+" read newrow1");
			TransactionHandler t2 = xactManager.startTransaction();
			Put p = new Put(Bytes.toBytes("SIrow2"));
			p.add(Bytes.toBytes("d"),Bytes.toBytes("c1"),Bytes.toBytes("val-1"));
			t1.put("table1", p);
			t2.put("table1", p);
			if(t1.commit())
				System.out.println("Transaction#"+t1.getTid()+" committed with commit_ts"+t1.getCommitTS());
			else
				System.out.println("Transaction#"+t1.getTid()+" aborted");
			
			if(t2.commit())
				System.out.println("Transaction#"+t2.getTid()+" committed with commit_ts"+t2.getCommitTS());
			else
				System.out.println("Transaction#"+t2.getTid()+" aborted");
			
			
		}catch(Exception e){
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private void serializabilityTest(){
		System.out.println("********Performing Serializability test******");
		try{
			TransactionHandler t0 = xactManager.startTransaction();
			Put px0 = new Put(Bytes.toBytes("A8"));
			px0.add(Bytes.toBytes("d"),Bytes.toBytes("c1"),Bytes.toBytes("val-T0"));			
			Put py0 = new Put(Bytes.toBytes("B8"));
			py0.add(Bytes.toBytes("d"),Bytes.toBytes("c1"),Bytes.toBytes("val-T0"));			
			t0.put("table1", px0);
			t0.put("table1", py0);
			t0.commit();
			
			
			TransactionHandler t1 = xactManager.startTransaction();
			Put px1 = new Put(Bytes.toBytes("A8"));
			px1.add(Bytes.toBytes("d"),Bytes.toBytes("c1"),Bytes.toBytes("val-T1"));
			t1.put("table1", px1);
			
			TransactionHandler t2 = xactManager.startTransaction();
			Get g = new Get(Bytes.toBytes("A8"));
			g.addColumn(Bytes.toBytes("d"),Bytes.toBytes("c1"));
			Result r = t2.get("table1", g);
			System.out.println("Transaction#"+t2.getTid()+" read A="+Bytes.toString(r.value()));
			
			t1.commit();
			
			Put py2 = new Put(Bytes.toBytes("B8"));
			py2.add(Bytes.toBytes("d"),Bytes.toBytes("c1"),Bytes.toBytes("val-T2"));
			t2.put("table1", py2);
			
			TransactionHandler t3 = xactManager.startTransaction();
			Get gx3 = new Get(Bytes.toBytes("A8"));
			gx3.addColumn(Bytes.toBytes("d"),Bytes.toBytes("c1"));
			r = t3.get("table1", g);
			System.out.println("Transaction#"+t3.getTid()+" read A="+Bytes.toString(r.value()));
			
			t2.commit();
				
			
			Get g2 = new Get(Bytes.toBytes("B8"));
			g2.addColumn(Bytes.toBytes("d"),Bytes.toBytes("c1"));
			r = t3.get("table1", g2);
			System.out.println("Transaction#"+t3.getTid()+" read B="+Bytes.toString(r.value()));
			t3.commit();
			
		}catch(Exception e){
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private void basicTest(){
		System.out.println("********Performing basic test******");
		long sumExecTime = 0;	//total txn execution
		
		for(int i=0; i<100;i++){
			try{
				long start = System.currentTimeMillis();
				TransactionHandler t = xactManager.startTransaction();				
				System.out.println("Transaction#"+i+" started with tid:"+t.getTid()+" with snapshot_ts:"+t.getSnapshotTS());
				Get g = new Get(Bytes.toBytes("btest-run11"));
				g.addColumn(Bytes.toBytes("d"),Bytes.toBytes("c1"));
				Result r = t.get("table1", g);
				if(r!=null)
					System.out.println("Transaction#"+i+" read value:"+Bytes.toString(r.value()));
				Put p = new Put(Bytes.toBytes("btest-run11"));
				p.add(Bytes.toBytes("d"),Bytes.toBytes("c1"),Bytes.toBytes("val-"+(i+1)));
				t.put("table1", p);
				if(t.commit())
					System.out.println("Transaction#"+i+" committed with commit_ts"+t.getCommitTS());
				else {
					System.out.println("Transaction#"+i+" failed");					
				}
				long delay = System.currentTimeMillis() - start;
				sumExecTime+= delay;
			}catch(Exception e){
				System.out.println("Exception in running transaction #"+i);
				e.printStackTrace();
				System.exit(0);
			}			
		}
		
	}
	
	private void concurrencyTest(){
		System.out.println("********Performing concurrency test******");	
		TxnThread[] threads = new TxnThread[10];
		StatsManager.reportStartTime(System.currentTimeMillis());
		try{
		for(int i=0; i<100;i++){
		
				TxnThread thread = new TxnThread(i,pid);
				System.out.println("Starting TxnThread..");
				threads[i] = thread;
				thread.start();
				Thread.sleep(50);
		
		}
		for(int i=0; i<10;i++){
			threads[i].join();
		}
		}catch(Exception e){
			e.printStackTrace();		
		}
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		//System.out.println("Creating tables..");
		//Setup.CreateTables();
		//System.out.println("Resetting...");
		//Setup.reset();
		String configFile = args[0];
		Config config = new Config(configFile);
		
		ServerProcess server = new ServerProcess();
		//server.pid = Integer.parseInt(args[1]);
		config.getLogger().logInfo("ServerProcess started");
		server.run();
	}
	
	class TxnThread extends Thread{	
		int id = 0;
		int pid = 0;
		
		public TxnThread(int id, int pid) {
			this.id = id;
			this.pid = pid;
		}
		
		public void run(){
			TransactionManager xactManager = new TransactionManager();
			long sumExecTime = 0;	//total txn execution
			long sumCmtTime = 0;
			Random rnd = new Random();
			int count = 100;
			for(int i=0; i<count;i++) {
				try{
					long start = System.currentTimeMillis();
					TransactionHandler t = xactManager.startTransaction();				
					System.out.println("Transaction started with tid:"+t.getTid()+" with snapshot_ts:"+t.getSnapshotTS());
					Vector<Integer> num = new Vector<Integer>();
					for(int j=0;j<10;j++) {
						num.add(rnd.nextInt(100000));
					}				
					System.out.println();
					Random rnd2 = new Random();										
					for(int j=0;j<5;j++) {
						char prefix = (char)(rnd2.nextInt(26)+97);
						Get g = new Get(Bytes.toBytes(prefix+"-run7-"+num.get(j)));
						g.addColumn(Bytes.toBytes("d"),Bytes.toBytes("c1"));
						Result r = t.get("table1", g);
						if(r !=null)
							System.out.println("Transaction#"+t.getTid()+" read value:"+Bytes.toString(r.value()));
						Put p = new Put(Bytes.toBytes(prefix+"-run7-"+num.get(j+5)));
						p.add(Bytes.toBytes("d"),Bytes.toBytes("c1"),Bytes.toBytes("val-"+i));
						t.put("table1", p);
					}
					long cmtStart = System.currentTimeMillis();
					boolean cmt = t.commit();
					long cmtDelay = System.currentTimeMillis() - cmtStart;
					if(cmt)
						System.out.println("<ServerProcess> Transaction#"+t.getTid()+" committed with commit_ts:"+t.getCommitTS()+" "+num);
					else {
						System.out.println("<ServerProcess> Transaction#"+t.getTid()+" failed "+num);					
					}
					long delay = System.currentTimeMillis() - start;
					sumExecTime += delay;
					sumCmtTime += cmtDelay;
					//Thread.sleep(50);
				}catch(Exception e){
					System.out.println("Exception in running transaction #"+i);
					e.printStackTrace();
					return;
				}			
			}
			
		}
	}
	
	
}
