package service;

import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

import xact.*;
import util.*;

public class AccountBenchmark {
	/*
	 * joint-account withdraw benchmark, emulates write-skew and read-only transaction anomalies
	 */
	public static final int TABLE_SIZE = 10;	//number of items
	
	
	String option;
	public AccountBenchmark(String option) {
		this.option = option;
	}
	
	public void run() throws Exception{
		if(option.equals("-l")) {
			loadTable();
		}
		else if(option.equals("-c")) {
			emptyTable();
		}
		else if(option.equals("-v")) {
			verify();
		}
		else if(option.equals("-r")) {
			StatsManager.reportStartTime(System.currentTimeMillis());
			LoggerThread lg = new LoggerThread();
			lg.start();
			String[] tableNames = {"account"};
			Batcher.startNewBatcher(Arrays.asList(tableNames));
			int count = 5;
			BenchmarkThread threads[] = new BenchmarkThread[count];
			//for(int j=0; j < 1200; j++) {
				for(int i=0; i<count;i++){
					BenchmarkThread thread = new BenchmarkThread();
					threads[i] = thread;
					thread.start();
					Thread.sleep(25);
					//System.out.println("staring new thread");
				}
			//}
			
			//for(int i=0; i<count;i++){
				//threads[i].join();
			//}
			//StatsManager.logStats();
			
			
		}else{
			System.out.println("wrong option, Usage: <class name> -c(recreate table) | -l(load table) | -r(run)");
		}
		
	}
	
	public static void main(String args[]) throws Exception{
		String configFile = args[0];
		Config config = new Config(configFile);
		
		AccountBenchmark bmark = new AccountBenchmark(args[1]);
		config.getLogger().logInfo("AccountBenchmark started...");
		
		bmark.run();
	}
	
	public void loadTable() throws IOException{
		System.out.println("loading table");
		Configuration conf = HBaseConfiguration.create();
		HTable hTable = new HTable(conf,"account");
		hTable.setAutoFlush(false);
		for(long i=0; i< TABLE_SIZE;i++){
			Put p = new Put(Bytes.toBytes(i),1);
			p.add(Bytes.toBytes("d"), Bytes.toBytes("bal"), Bytes.toBytes(50));
			p.add(Bytes.toBytes("md"),Bytes.toBytes("tid"),Bytes.toBytes(1));
			p.add(Bytes.toBytes("md"),Bytes.toBytes("lock"),System.currentTimeMillis(),Bytes.toBytes(-1));
			hTable.put(p);
			if(i%100==0) {
				hTable.flushCommits();
			}			
		}
		
		System.out.println("loading table finished");
		hTable.flushCommits();
		hTable.setAutoFlush(true);
		String[] tableNames = {"account"};
		Batcher.startNewBatcher(Arrays.asList(tableNames));
		TransactionManager tManager = new TransactionManager();
		TransactionHandler t = tManager.startTransaction(); //dummy transaction
		t.commit();
		System.out.println("created root transaction");
	}
	
	public void emptyTable() throws IOException {	    
		Configuration conf = HBaseConfiguration.create();
		
		HBaseAdmin admin = new HBaseAdmin(conf);		
	    		
		
		HColumnDescriptor colFam_D = new HColumnDescriptor("d");
		colFam_D.setMaxVersions(20);		
		HColumnDescriptor colFam_MD = new HColumnDescriptor("md");
		colFam_MD.setMaxVersions(20);
		HTableDescriptor table = new HTableDescriptor("account");
		table.addFamily(colFam_D);
		table.addFamily(colFam_MD);
		if (admin.tableExists(table.getName())) {
			admin.disableTable(table.getName());
			admin.deleteTable(table.getName());
		} 
		admin.createTable(table);
		System.out.println("account table created");
		
		int numSplitsLess = 4;
		int numSplitsMore = 12;
		byte[][] splitKeysLess = new byte[numSplitsLess-1][]; 
		byte[][] splitKeysMore = new byte[numSplitsMore-1][];
		
		int offset = Integer.MAX_VALUE/(numSplitsLess);
		int left = 0;
		for(int i=0; i<numSplitsLess-1;i++){
			int right = left+offset;
			splitKeysLess[i]= Bytes.toBytes(right); 
			left = right;	
		}
		offset = Integer.MAX_VALUE/(numSplitsMore);
		left = 0;
		for(int i=0; i<numSplitsMore-1;i++){
			int right = left+offset;
			splitKeysMore[i]= Bytes.toBytes(right); 
			left = right;	
		}		
	}
	
	public void verify() throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable hTable = new HTable(conf,"account");		 
		Scan scan = new Scan(Bytes.toBytes(0), Bytes.toBytes(TABLE_SIZE/2));
		scan.addColumn(Bytes.toBytes("d"),Bytes.toBytes("bal"));
		scan.addColumn(Bytes.toBytes("md"),Bytes.toBytes("tid"));
		scan.addColumn(Bytes.toBytes("md"),Bytes.toBytes("rs"));
		ResultScanner rs = hTable.getScanner(scan);
	    Result r = null;
	    while( (r = rs.next()) !=null) {
	    	int xAmt = Bytes.toInt(r.getValue(Bytes.toBytes("d"),Bytes.toBytes("bal")));
	    	int xAct = Bytes.toInt(r.getRow());
	    	int yAct = xAct + TABLE_SIZE/2;
	    	Get g = new Get(Bytes.toBytes(yAct));
	    	//g.setTimeRange(0, 1);
	    	//g.setMaxVersions(1);
	    	g.addColumn(Bytes.toBytes("d"),Bytes.toBytes("bal"));
	    	g.addColumn(Bytes.toBytes("md"),Bytes.toBytes("tid"));
	    	g.addColumn(Bytes.toBytes("md"),Bytes.toBytes("rs"));
	    	Result r2 = hTable.get(g);
	    	int yAmt = Bytes.toInt(r2.value());
	    	//System.out.println("Accts:"+xAct+" "+yAct+" tids:"+Bytes.toInt(r.getValue(Bytes.toBytes("md"),Bytes.toBytes("tid")))+" "
	    			//+Bytes.toInt(r2.getValue(Bytes.toBytes("md"),Bytes.toBytes("tid"))));
	    	//System.out.println("Accts:"+xAct+" "+yAct+" bals:"+Bytes.toInt(r.getValue(Bytes.toBytes("d"),Bytes.toBytes("bal")))+" "
	    			//+Bytes.toInt(r2.getValue(Bytes.toBytes("d"),Bytes.toBytes("bal"))));
	    	
	    	if(xAmt+yAmt < 0) {
	    		System.out.println("Consistency violated, Joint Acct: ["+xAct+","+yAct+"]"+ "Balance:"+"("+xAmt+"+"+yAmt+")");
	    		System.out.println("Accts:"+xAct+" "+yAct+" tids:"+Bytes.toInt(r.getValue(Bytes.toBytes("md"),Bytes.toBytes("tid")))+" "
    			+Bytes.toInt(r2.getValue(Bytes.toBytes("md"),Bytes.toBytes("tid")))+ " rs:"+
    			Bytes.toString(r.getValue(Bytes.toBytes("md"),Bytes.toBytes("rs")))+" "
    			+ Bytes.toString(r2.getValue(Bytes.toBytes("md"),Bytes.toBytes("rs"))));
	    		System.out.println("Accts:"+xAct+" "+yAct+" bals:"+Bytes.toInt(r.getValue(Bytes.toBytes("d"),Bytes.toBytes("bal")))+" "
    			+Bytes.toInt(r2.getValue(Bytes.toBytes("d"),Bytes.toBytes("bal"))));
	    	}
	    }
	}
}

class LoggerThread extends Thread {
	public void run() {
		for(int i=0; i<3; i++){
			try {
				Thread.sleep(60*1000);
				StatsManager.logStats();
				StatsManager.reset();			
			}catch(Exception e){}
		}
		System.exit(0);
	}
}

class BenchmarkThread extends Thread {
	
	static final int DEP_PROB = 100;
	static final int WDRAW_PROB = 25;
	static final int CHECK_PROB = 50;
	
	TransactionManager xactManager;
	String tableName = "";
	static Random rndAcc;
	static Random rndPut;
	static Random rndAmt;
	static Random rndTxn;
	
	static {
		rndAcc = new Random();
		rndPut = new Random();
		rndAmt = new Random();
		rndTxn = new Random();
	}
	
	public BenchmarkThread() {
		xactManager = new TransactionManager();
		tableName = "account";		
	}
	
	public void run() {
		long period = 1000;		
		// for(int i=0; i<100;i++) {
		while(true) {	
			try {
				long start = System.currentTimeMillis();
				int prob = rndTxn.nextInt(100);
				if(prob < DEP_PROB){
					deposit();
				}else if(prob < (DEP_PROB+CHECK_PROB)){
					withdraw();
				}else{
					checkBalance();
				}
				long delay = System.currentTimeMillis() - start;
				if(delay < period)
					Thread.sleep(period-delay);	
				Config.getInstance().getLogger().logInfo("<AccountBenchmark> Executed transaction");
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		
	}	
	
	private void withdraw() throws IOException {	
		// System.out.println("Withdraw transaction for ");
		long xAct = rndAcc.nextInt(AccountBenchmark.TABLE_SIZE / 2);
		long xAct2 = xAct + 1;
		long xAct3 = xAct + 2;
		long yAct = xAct + (AccountBenchmark.TABLE_SIZE / 2);
		long yAct2 = xAct2 + (AccountBenchmark.TABLE_SIZE / 2);
		long yAct3 = xAct3 + (AccountBenchmark.TABLE_SIZE / 2);
		boolean committed = false;
		// while(!committed) {
			TransactionHandler t = xactManager.startTransaction();
		
			doWdraw(xAct, yAct, t);
			doWdraw(xAct2, yAct2, t);
			doWdraw(xAct3, yAct3, t);
		
			committed = t.commit(); 
		// }		
	}
	
	private void deposit() throws IOException {
		// System.out.println("Deposit transaction for ");
		long xAct = rndAcc.nextInt(AccountBenchmark.TABLE_SIZE / 2);
		long xAct2 = xAct +1;
		long xAct3 = xAct +2;
		long yAct = xAct + (AccountBenchmark.TABLE_SIZE / 2);
		long yAct2 = xAct2 + (AccountBenchmark.TABLE_SIZE / 2);
		long yAct3 = xAct3 + (AccountBenchmark.TABLE_SIZE / 2);
		boolean committed = false;
		// while(!committed){
			TransactionHandler t = xactManager.startTransaction();	
		
			doDeposit(xAct, yAct, t);
			doDeposit(xAct2, yAct2, t);
			doDeposit(xAct3, yAct3, t);
		
			 committed = t.commit(); 
		// }
		
	}
	
	private void checkBalance() throws IOException{
		//System.out.println("CheckBalance transaction for "+xAct+" and "+yAct);
		long xAct = rndAcc.nextInt(AccountBenchmark.TABLE_SIZE / 2);
		long yAct = xAct + (AccountBenchmark.TABLE_SIZE / 2);
		Get g1 = new Get(Bytes.toBytes(xAct));
		Get g2 = new Get(Bytes.toBytes(yAct));
		g1.addColumn(Bytes.toBytes("d"), Bytes.toBytes("bal"));
		g2.addColumn(Bytes.toBytes("d"), Bytes.toBytes("bal"));
		
		TransactionHandler t = xactManager.startTransaction();		
		Result r1 = t.get(tableName, g1);
		Result r2 = t.get(tableName, g2);
		t.commit();		
		
	}
	
	
	private void doWdraw(long xAct, long yAct, TransactionHandler t) throws IOException{
		Get g1 = new Get(Bytes.toBytes(xAct));
		Get g2 = new Get(Bytes.toBytes(yAct));
		g1.addColumn(Bytes.toBytes("d"), Bytes.toBytes("bal"));
		g2.addColumn(Bytes.toBytes("d"), Bytes.toBytes("bal"));
		
		long putAct = 0;
		long otherAct = 0;
		if(rndPut.nextBoolean()) { 
			putAct = xAct;
			otherAct = yAct;
		}
		else {
			putAct = yAct;
			otherAct = xAct;
		}
		
		
		
		Result r1 = t.get(tableName, g1);
		Result r2 = t.get(tableName, g2);
		if( r1 ==null || r1.isEmpty() || r2==null || r2.isEmpty()) {			
			return;			
		}
		int amtX = 0 ;
		int amtY = 0;
		if(!r1.isEmpty() && r2.isEmpty()){
			amtX = Bytes.toInt(r1.getValue(Bytes.toBytes("d"), Bytes.toBytes("bal")));
			amtY = Bytes.toInt(r2.getValue(Bytes.toBytes("d"), Bytes.toBytes("bal")));
		}
		
		if(amtX+amtY > 0) {
			int newBal;
			if(putAct == xAct)
				newBal = amtX - (amtX+amtY);
			else	
				newBal = amtY - (amtX+amtY);
			
			Put p1 = new Put(Bytes.toBytes(putAct));			
			p1.add(Bytes.toBytes("d"), Bytes.toBytes("bal"), Bytes.toBytes(newBal));
			t.put(tableName, p1);
			
			int otherBal = 0;
			Put p2 = new Put(Bytes.toBytes(otherAct));			
			p2.add(Bytes.toBytes("d"), Bytes.toBytes("bal"), Bytes.toBytes(otherBal));
			t.put(tableName, p2);
			
			
		}
		
	}
	
	
	
	private void doDeposit(long xAct, long yAct, TransactionHandler t) throws IOException{
		long putAct = 0;
		if(rndPut.nextBoolean()) 
			putAct = xAct;
		else
			putAct = yAct;
		
		Get g = new Get(Bytes.toBytes(putAct));
		g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("bal"));
		Put p = new Put(Bytes.toBytes(putAct));
		
				
		Result r = t.get(tableName, g);
		int bal;
		if(r.containsColumn(Bytes.toBytes("d"), Bytes.toBytes("bal"))){
			bal = Bytes.toInt(r.getValue(Bytes.toBytes("d"), Bytes.toBytes("bal")));
		}else
			bal = 0;
		
		int depAmt = rndAmt.nextInt(1000)+1;
		int newBal = bal + depAmt;
		p.add(Bytes.toBytes("d"), Bytes.toBytes("bal"), Bytes.toBytes(newBal));
		t.put(tableName, p);
	}
	
}
