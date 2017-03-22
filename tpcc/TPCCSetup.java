package tpcc;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.util.Bytes;

import xact.*;
import util.*;

import java.util.*;
import java.util.concurrent.*;
import java.math.*;

public class TPCCSetup {
	Configuration conf;
	HBaseAdmin admin;

	String[] C_LAST = { "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI",
			"CALLY", "ATION", "EING" };

	static int ITEM_SIZE = 100000;
	static int NUM_WHOUSE = 1;
	static int DIST_SCALE = 10;
	static int CUST_SCALE = 3000;
	static int HIST_SIZE = 3000;
	static int INIT_ORDER_COUNT = 3000;
	static long ORDER_SCALE = 1000000000;	// max 1 billion orders per warehouse
	static long NEWORDER_SCALE = 900;
	static long ORDLINE_SIZE = 30000;
	static long STOCK_SCALE = 100000;
	
	long WH_COUNT = NUM_WHOUSE;
	long DIST_COUNT = NUM_WHOUSE * DIST_SCALE;
	long ITEM_COUNT = ITEM_SIZE;
	long CUST_COUNT = NUM_WHOUSE * DIST_SCALE * CUST_SCALE;
	long STOCK_COUNT = NUM_WHOUSE * STOCK_SCALE;
	long ORDER_COUNT = NUM_WHOUSE * DIST_SCALE * ORDER_SCALE;
	long NEWORDER_COUNT = NUM_WHOUSE * DIST_SCALE * NEWORDER_SCALE;

	static int numMachines=4;
	
	HashMap<String, Long> tableSizes;
	
	private void calcRowCounts() {		
		tableSizes = new HashMap<String, Long>();
		WH_COUNT = NUM_WHOUSE;
		tableSizes.put("WAREHOUSE", WH_COUNT);		
		DIST_COUNT = NUM_WHOUSE * DIST_SCALE;
		tableSizes.put("DISTRICT", DIST_COUNT);
		ITEM_COUNT = ITEM_SIZE;
		tableSizes.put("ITEM", ITEM_COUNT);
		CUST_COUNT = NUM_WHOUSE * DIST_SCALE * CUST_SCALE;
		tableSizes.put("CUSTOMER", CUST_COUNT);
		STOCK_COUNT = NUM_WHOUSE * STOCK_SCALE;
		tableSizes.put("STOCK", STOCK_COUNT);
		ORDER_COUNT = NUM_WHOUSE * DIST_SCALE * ORDER_SCALE;
		tableSizes.put("ORDER", ORDER_COUNT);
		tableSizes.put("ORDER-LINE", ORDER_COUNT*100);
		NEWORDER_COUNT = ORDER_COUNT;		
		tableSizes.put("NEW-ORDER", NEWORDER_COUNT);		
	}	
	
	private void createTable(String tableName, long rowCount) throws IOException {
		HColumnDescriptor colFam_D = new HColumnDescriptor("d");
		colFam_D.setMaxVersions(100);
		colFam_D.setInMemory(true);
		HColumnDescriptor colFam_MD = new HColumnDescriptor("md");
		colFam_MD.setMaxVersions(100);
		colFam_MD.setInMemory(true);

		int numRegions;	//number of regions per table
		long start, end, regionSize;	//start=>end key for first region, end=>start key for last region
		byte[] startK, endK;
		
		numRegions = numMachines;
		System.out.println("Row count for "+tableName+" "+rowCount);
		regionSize = rowCount/(long)numRegions;
		start = 0 + regionSize;
		end = rowCount - regionSize;
		startK = Bytes.toBytes(start);
		endK = Bytes.toBytes(end);
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		tableDesc.addFamily(colFam_D);
		tableDesc.addFamily(colFam_MD);
		System.out.println("Creating table "+tableName+" with "+numRegions+" regions, start region: 0-"+start+" end region:"+end+"-"+rowCount);
		admin.createTable(tableDesc, startK, endK, numRegions);
		
	}
	
	public  void createTables() {		
		// create tables
		try{
			createTable("WAREHOUSE", WH_COUNT);
			createTable("DISTRICT", DIST_COUNT);
			createTable("ORDER", ORDER_COUNT);
			createTable("CUSTOMER", CUST_COUNT);
			createTable("STOCK", STOCK_COUNT);
			createTable("NEW-ORDER", NEWORDER_COUNT);
			createTable("ORDER-LINE", ORDER_COUNT*100);
			createTable("ITEM", ITEM_COUNT);
			/*
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
			
			HTableDescriptor cmt = new HTableDescriptor("CMTLOG");
			colFam_D.setMaxVersions(1);
			cmt.addFamily(colFam_D);		
			admin.createTable(cmt);		
			System.out.println("CMTLOG table created");			
			
			/*
			HTableDescriptor precmt = new HTableDescriptor("PRECMT");
			colFam_D.setMaxVersions(1);
			precmt.addFamily(colFam_D);		
			admin.createTable(precmt);		
			System.out.println("PRECMT table created");
			*/
			
			/*
			HTableDescriptor committing = new HTableDescriptor("COMMITTING");
			colFam_D.setMaxVersions(1);
			committing.addFamily(colFam_D);		
			admin.createTable(committing,splitKeysMore);		
			System.out.println("COMMITTING table created");
			*/
			
		}catch(IOException e){
			e.printStackTrace();
			System.out.print("Error in creating tables..");
			System.exit(0);
		}		
	}
	
	public void populateTable(String table, int startPart, int endPart) throws Exception {
		System.out.println("Populating table "+table+" partition:["+startPart+"-"+endPart+")");
		if (table.equals("ITEM")) {
			populateItemTable();
		} else if (table.equals("WAREHOUSE")) {
			populateWarehouse(startPart, endPart);
		} else if (table.equals("DISTRICT")) {
			populateDistrict(startPart, endPart);
		} else if (table.equals("STOCK")) {
			populateStock(startPart, endPart);
		} else if (table.equals("CUSTOMER")) {
			populateCustomer(startPart, endPart);
		} else if (table.equals("ORDER")) {
			populateOrder(startPart, endPart);
		} else if (table.equals("NEWORDER")) { 
			populateNewOrder(startPart, endPart);
		}
	}
	
	public void populateAll(int startPart, int endPart) throws Exception{
		String tableNames[] = {"WAREHOUSE", "DISTRICT", "STOCK", "CUSTOMER", "ORDER", "NEWORDER"};
		populate(startPart, endPart, tableNames);
	}

	public void populate(int startPart, int endPart, String tableNames[]) throws Exception {		
		ExecutorService executor = Executors.newFixedThreadPool(tableNames.length);
		for (int i=0; i<tableNames.length; i++) {			
			final String table = tableNames[i];
			final int start = startPart;
			final int end = endPart;
			executor.submit(new Runnable() {			
				public void run() {
					try {
						populateTable(table, start, end);
					} catch(Exception e) {
						e.printStackTrace();
					}
				}
			});		
		}
		executor.shutdown();
		executor.awaitTermination(20, TimeUnit.MINUTES);
	}
	
	public void populateItemTable() throws IOException {
		HTable hTable = new HTable(conf,"ITEM");
		hTable.setAutoFlush(false);
		Random rndSource = new Random();
		Random rndNum = new Random();			
		for(long i=0; i< ITEM_SIZE;i++){
			Put p = new Put(Bytes.toBytes(i),1);
			int id = rndSource.nextInt(10000)+1;
			p.add(Bytes.toBytes("d"), Bytes.toBytes("i_id"), Bytes.toBytes(id));
			int length = rndNum.nextInt(11)+14;
			byte[] name = new byte[length];
			rndSource.nextBytes(name);
			p.add(Bytes.toBytes("d"), Bytes.toBytes("i_name"), name);
			float price = rndSource.nextFloat()*99 + 1;
			p.add(Bytes.toBytes("d"), Bytes.toBytes("i_price"), Bytes.toBytes(price));
			length = rndNum.nextInt(26)+25;
			byte[] data = new byte[length];
			rndSource.nextBytes(data);
			p.add(Bytes.toBytes("d"), Bytes.toBytes("i_data"), data);
			p.add(Bytes.toBytes("md"),Bytes.toBytes("tid"),Bytes.toBytes(1));
			p.add(Bytes.toBytes("md"),Bytes.toBytes("lock"),System.currentTimeMillis(),Bytes.toBytes(-1));
			hTable.put(p);
			if(i%100==0) {
				// hTable.flushCommits();
			}			
		}
		
		System.out.println("loading table ITEM finished");
		hTable.flushCommits();	
	}
	
	public void populateWarehouse(int startPart, int endPart) throws IOException{		
		HTable hTable = new HTable(conf,"WAREHOUSE");
		hTable.setAutoFlush(false);
		Random rndSource = new Random();
		Random rndNum = new Random();			
		for(long i=startPart; i< endPart;i++){
			Put p = new Put(Bytes.toBytes(i),1);
			int length = rndNum.nextInt(5)+6;
			byte[] name = new byte[length];
			rndSource.nextBytes(name);
			p.add(Bytes.toBytes("d"), Bytes.toBytes("w_name"), name);
			length=rndNum.nextInt(11)+10;
			byte[] street1 = new byte[length];
			rndSource.nextBytes(street1);
			p.add(Bytes.toBytes("d"), Bytes.toBytes("w_street1"), street1);
			length=rndNum.nextInt(11)+10;
			byte[] street2 = new byte[length];
			rndSource.nextBytes(street2);
			p.add(Bytes.toBytes("d"), Bytes.toBytes("w_street2"), street2);
			length=rndNum.nextInt(11)+10;
			byte[] city = new byte[length];
			rndSource.nextBytes(city);
			p.add(Bytes.toBytes("d"), Bytes.toBytes("w_city"), city);				
			byte[] state = new byte[2];
			rndSource.nextBytes(state);
			p.add(Bytes.toBytes("d"), Bytes.toBytes("w_state"), state);
			long zip = (long) (rndSource.nextDouble() * 1000000000);
			p.add(Bytes.toBytes("d"), Bytes.toBytes("w_zip"), Bytes.toBytes(zip));
			double tax = rndSource.nextDouble()*2;
			p.add(Bytes.toBytes("d"), Bytes.toBytes("w_tax"), Bytes.toBytes(tax));
			p.add(Bytes.toBytes("d"), Bytes.toBytes("w_ytd"), Bytes.toBytes(300000));
			p.add(Bytes.toBytes("md"),Bytes.toBytes("tid"),Bytes.toBytes(1));
			p.add(Bytes.toBytes("md"),Bytes.toBytes("lock"),System.currentTimeMillis(),Bytes.toBytes(-1));
			hTable.put(p);
			if(i%100==0) {
				// hTable.flushCommits();
			}			
		}
		hTable.flushCommits();
		System.out.println("loading table WAREHOUSE finished for partition:["+startPart+"-"+endPart+")");
					 
	}
	
	public void populateDistrict(int startPart, int endPart) throws IOException{
		HTable hTable = new HTable(conf,"DISTRICT");
		hTable.setAutoFlush(false);
		Random rndSource = new Random();
		Random rndNum = new Random();			
		for(long i=startPart; i< endPart;i++) {
			for(long j=0;j< DIST_SCALE;j++) {
				long id = i*DIST_SCALE+j;
				Put p = new Put(Bytes.toBytes(id),1);
				//p.add(Bytes.toBytes("d"), Bytes.toBytes("d_w_id"), Bytes.toBytes(i));
				int length = rndNum.nextInt(5)+6;
				byte[] name = new byte[length];
				rndSource.nextBytes(name);
				p.add(Bytes.toBytes("d"), Bytes.toBytes("d_name"), name);
				length=rndNum.nextInt(11)+10;
				byte[] street1 = new byte[length];
				rndSource.nextBytes(street1);
				p.add(Bytes.toBytes("d"), Bytes.toBytes("d_street1"), street1);
				length=rndNum.nextInt(11)+10;
				byte[] street2 = new byte[length];
				rndSource.nextBytes(street2);
				p.add(Bytes.toBytes("d"), Bytes.toBytes("d_street2"), street2);
				length=rndNum.nextInt(11)+10;
				byte[] city = new byte[length];
				rndSource.nextBytes(city);
				p.add(Bytes.toBytes("d"), Bytes.toBytes("d_city"), city);				
				byte[] state = new byte[2];
				rndSource.nextBytes(state);
				p.add(Bytes.toBytes("d"), Bytes.toBytes("d_state"), state);
				long zip = (long) (rndSource.nextDouble() * 1000000000);
				p.add(Bytes.toBytes("d"), Bytes.toBytes("d_zip"), Bytes.toBytes(zip));
				double tax = rndSource.nextDouble()*2;
				p.add(Bytes.toBytes("d"), Bytes.toBytes("d_tax"), Bytes.toBytes(tax));
				p.add(Bytes.toBytes("d"), Bytes.toBytes("d_ytd"), Bytes.toBytes(300000));
				p.add(Bytes.toBytes("d"), Bytes.toBytes("d_next_oid"), Bytes.toBytes((long)3001));
				p.add(Bytes.toBytes("md"),Bytes.toBytes("tid"),Bytes.toBytes(1));
				p.add(Bytes.toBytes("md"),Bytes.toBytes("lock"),System.currentTimeMillis(),Bytes.toBytes(-1));
				hTable.put(p);
				if(j%100==0) {
					// hTable.flushCommits();
				}			
			}
		}
		hTable.flushCommits();
		System.out.println("loading table DISTRICT finished for partition:["+startPart+"-"+endPart+")");
	}
	
	public void populateStock(int startPart, int endPart) throws Exception{		
		ExecutorService executors = Executors.newFixedThreadPool(10);
		Random rndSource = new Random();
		Random rndNum = new Random();		
		List<Put> putList = new Vector<Put>();
		HTable table = new HTable(conf, "STOCK");
		table.setAutoFlush(false);
		for(long i=startPart; i< endPart;i++){			
			for(long j=0;j< STOCK_SCALE;j++){
				long id=i*STOCK_SCALE+j;
				Put p = new Put(Bytes.toBytes(id),1);
				//p.add(Bytes.toBytes("d"), Bytes.toBytes("s_w_id"), Bytes.toBytes(i));
				int qty = rndSource.nextInt(91)+10;
				p.add(Bytes.toBytes("d"), Bytes.toBytes("s_quantity"), Bytes.toBytes(qty));
				for(int k=0; k<DIST_SCALE;k++){
					byte[] dist = new byte[24];
					rndSource.nextBytes(dist);
					String dist_num = "s_dist"+(k+1);
					p.add(Bytes.toBytes("d"), Bytes.toBytes(dist_num), dist);
				}
				p.add(Bytes.toBytes("d"), Bytes.toBytes("s_ytd"), Bytes.toBytes(0));
				p.add(Bytes.toBytes("d"), Bytes.toBytes("d_order_cnt"), Bytes.toBytes(0));
				p.add(Bytes.toBytes("d"), Bytes.toBytes("d_remove_cnt"), Bytes.toBytes(0));
				int length = rndNum.nextInt(26)+25;
				byte[] data = new byte[length];
				rndSource.nextBytes(data);
				p.add(Bytes.toBytes("d"), Bytes.toBytes("s_data"), data);
				p.add(Bytes.toBytes("md"),Bytes.toBytes("tid"),Bytes.toBytes(1));
				p.add(Bytes.toBytes("md"),Bytes.toBytes("lock"),System.currentTimeMillis(),Bytes.toBytes(-1));
				//putList.add(p);
				table.put(p);
				/*
				if((i*j)%1000==0) {
					final List<Put> puts = putList;
					putList = new Vector<Put>();
					//System.out.println(Thread.currentThread().activeCount());
					executors.submit(new Runnable() {
						public void run() {	
							try {
								HTable table = new HTable(conf, "STOCK");
								table.put(puts);
							}catch(Exception e){e.printStackTrace();}
						}
					});
				}
				*/			
			}
		}
		
		//table.put(putList);
		table.flushCommits();
		executors.shutdown();
		executors.awaitTermination(20, TimeUnit.MINUTES);		
		System.out.println("loading table STOCK finished for partition:["+startPart+"-"+endPart+")");
	}
	
	public void populateCustomer(int startPart, int endPart) throws Exception{
		ExecutorService executors = Executors.newFixedThreadPool(5);	
		Random rndSource = new Random();
		Random rndNum = new Random();	
		List<Put> putList = new Vector<Put>();
		HTable table = new HTable(conf, "CUSTOMER");
		table.setAutoFlush(false);
		for(long i=startPart; i< endPart;i++){
			for(long j=0;j< DIST_SCALE;j++){				
				for(long k=0;k<CUST_SCALE; k++){
					long id = i*DIST_SCALE*CUST_SCALE+j*CUST_SCALE+k;
					Put p = new Put(Bytes.toBytes(id),1);
					//p.add(Bytes.toBytes("d"), Bytes.toBytes("c_d_id"), Bytes.toBytes(j));
					//p.add(Bytes.toBytes("d"), Bytes.toBytes("c_w_id"), Bytes.toBytes(i));
					byte[] c_last = new byte[12];
					rndSource.nextBytes(c_last);
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_last"), c_last);
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_middle"), Bytes.toBytes("OE"));
					int length=rndNum.nextInt(9)+8;
					byte[] c_first = new byte[length];
					rndSource.nextBytes(c_first);
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_first"), c_first);
					length=rndNum.nextInt(11)+10;
					byte[] street1 = new byte[length];
					rndSource.nextBytes(street1);
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_street1"), street1);
					length=rndNum.nextInt(11)+10;
					byte[] street2 = new byte[length];
					rndSource.nextBytes(street2);
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_street2"), street2);
					length=rndNum.nextInt(11)+10;
					byte[] city = new byte[length];
					rndSource.nextBytes(city);
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_city"), city);				
					byte[] state = new byte[2];
					rndSource.nextBytes(state);
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_state"), state);
					long zip = (long) (rndSource.nextDouble() * (10^9));
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_zip"), Bytes.toBytes(zip));
					long phone = (long) (rndSource.nextDouble() * (10^16));
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_phone"), Bytes.toBytes(phone));
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_since"), Bytes.toBytes(System.currentTimeMillis()));
					String c_credit = "GC";
					if(rndNum.nextInt(100) > 89)
						c_credit = "BC";
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_credit"), Bytes.toBytes(c_credit));
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_credlimit"), Bytes.toBytes(50000));
					float disc= rndSource.nextFloat()*5;
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_discount"), Bytes.toBytes(disc));
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_balance"), Bytes.toBytes(-10));
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_ytd_pmt"), Bytes.toBytes(10.0));
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_pmt_cnt"), Bytes.toBytes(1));
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_delivery_cnt"), Bytes.toBytes(1));
					length=rndNum.nextInt(301)+200;
					byte[] data = new byte[length];
					rndSource.nextBytes(data);
					p.add(Bytes.toBytes("d"), Bytes.toBytes("c_data"), data);
					p.add(Bytes.toBytes("md"),Bytes.toBytes("tid"),Bytes.toBytes(1));
					p.add(Bytes.toBytes("md"),Bytes.toBytes("lock"),System.currentTimeMillis(),Bytes.toBytes(-1));
					//putList.add(p);
					table.put(p);
					
					/*
					if((i*j)%1000==0) {
						final List<Put> puts = putList;
						putList = new Vector<Put>();
						//System.out.println(Thread.currentThread().activeCount());
						
						executors.submit(new Runnable() {
							public void run() {	
								try {
									HTable table = new HTable(conf, "CUSTOMER");
									table.put(puts);
								}catch(Exception e){e.printStackTrace();}
							}
						});
					}
					*/			
				}
			}
		}
		
		//table.put(putList);
		table.flushCommits();
		executors.shutdown();
		executors.awaitTermination(20, TimeUnit.MINUTES);
		System.out.println("loading table CUSTOMER finished for partition:["+startPart+"-"+endPart+")");
	}
	
	public void populateOrder(int startPart, int endPart) throws IOException {
		HTable hTable = new HTable(conf,"ORDER");
		hTable.setAutoFlush(false);
		Random rndSource = new Random();
		Random rndNum = new Random();	
		HTable hTableLine = new HTable(conf,"ORDER-LINE");
		hTableLine.setAutoFlush(false);		
		for(long i=startPart; i< endPart;i++){
			for(long j=0;j< DIST_SCALE;j++){
				for(long k=0;k<INIT_ORDER_COUNT; k++){
					long id = i*DIST_SCALE*ORDER_SCALE+j*ORDER_SCALE+k;	
					Put p = new Put(Bytes.toBytes(id),1);
					//p.add(Bytes.toBytes("d"), Bytes.toBytes("c_d_id"), Bytes.toBytes(j));
					//p.add(Bytes.toBytes("d"), Bytes.toBytes("c_w_id"), Bytes.toBytes(i));
					p.add(Bytes.toBytes("d"), Bytes.toBytes("o_c_id"), Bytes.toBytes(id));
					p.add(Bytes.toBytes("d"), Bytes.toBytes("o_entry"), Bytes.toBytes(System.currentTimeMillis()));
					int ol_cnt = rndSource.nextInt(11)+5;
					p.add(Bytes.toBytes("d"), Bytes.toBytes("o_ol_cnt"), Bytes.toBytes(ol_cnt));
					for(int n=0;n<ol_cnt;n++){
						long ol_id = id*100 + n;
						//System.out.println("olid:"+ol_id);						
						Put pLine = new Put(Bytes.toBytes(ol_id),1);
						int item = rndSource.nextInt(100000);
						pLine.add(Bytes.toBytes("d"), Bytes.toBytes("ol_i_id"), Bytes.toBytes(item));
						pLine.add(Bytes.toBytes("d"), Bytes.toBytes("ol_supply_wid"), Bytes.toBytes(i));
						pLine.add(Bytes.toBytes("d"), Bytes.toBytes("ol_quantity"), Bytes.toBytes(5));
						float amt = rndSource.nextFloat()*9999;
						pLine.add(Bytes.toBytes("d"), Bytes.toBytes("ol_amount"), Bytes.toBytes(amt));
						pLine.add(Bytes.toBytes("md"),Bytes.toBytes("tid"),Bytes.toBytes(1));
						pLine.add(Bytes.toBytes("md"),Bytes.toBytes("lock"),System.currentTimeMillis(),Bytes.toBytes(-1));
						hTableLine.put(pLine);
					}
					// hTableLine.flushCommits();
					p.add(Bytes.toBytes("d"), Bytes.toBytes("o_all_local"), Bytes.toBytes(true));
					p.add(Bytes.toBytes("md"),Bytes.toBytes("tid"),Bytes.toBytes(1));
					p.add(Bytes.toBytes("md"),Bytes.toBytes("lock"),System.currentTimeMillis(),Bytes.toBytes(-1));
					hTable.put(p);
					if(k%100==0) {
						// hTable.flushCommits();
					}		
				}
			}
		}
		System.out.println("loading table ORDER-LINE finished for partition:["+startPart+"-"+endPart+")");
		hTable.flushCommits();
		System.out.println("loading table ORDER finished for partition:["+startPart+"-"+endPart+")");
	}
	
	public void populateNewOrder(int startPart, int endPart) throws IOException{
		HTable hTable = new HTable(conf,"NEW-ORDER");
		hTable.setAutoFlush(false);
		Random rndSource = new Random();
		Random rndNum = new Random();
		for(long i=startPart; i< endPart;i++){
			for(long j=0;j< DIST_SCALE;j++){
				for(long k=INIT_ORDER_COUNT-NEWORDER_SCALE;k<INIT_ORDER_COUNT; k++){
					long id = i*DIST_SCALE*ORDER_SCALE+j*ORDER_SCALE+k;
					Put p = new Put(Bytes.toBytes(id),1);
					p.add(Bytes.toBytes("md"),Bytes.toBytes("tid"),Bytes.toBytes(1));
					p.add(Bytes.toBytes("md"),Bytes.toBytes("lock"),System.currentTimeMillis(),Bytes.toBytes(-1));
					hTable.put(p);
					if(k%100==0) {
						// hTable.flushCommits();
					}		
				}
			}
		}
		hTable.flushCommits();
		System.out.println("loading table NEW-ORDER finished for partition:["+startPart+"-"+endPart+")");
	}
	
	public boolean verifyRowCount(String tableName, long expected) {
		AggregationClient aggrClient = new AggregationClient(Config.getInstance().getHBaseConfig());		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("d"));
		try {
			long count = aggrClient.rowCount(Bytes.toBytes(tableName), null, scan);
			if (count != NUM_WHOUSE) {
				System.out.println("Wrong number of rows in "+tableName+" table, expected:"+expected+" actual:"+count);
				return false;
			}
		}catch(Throwable t) {
			t.printStackTrace();
			return false;
		}
		return true;
	}
	
	public void verifyDB() {			
		calcRowCounts();
		verifyRowCount("ITEM", ITEM_COUNT);
		verifyRowCount("WAREHOUSE", WH_COUNT);
		verifyRowCount("DISTRICT", DIST_COUNT);
		verifyRowCount("STOCK", STOCK_COUNT);
		verifyRowCount("CUSTOMER", CUST_COUNT);
		verifyRowCount("ORDER", ORDER_COUNT);
		verifyRowCount("NEW-ORDER", NEWORDER_COUNT);
	}
	
	public void cleanLocks() {
		String tableNames[] = {"WAREHOUSE", "DISTRICT", "STOCK", "CUSTOMER", "ORDER", "NEW-ORDER", "ORDER-LINE", "ITEM"};
		ExecutorService executor = Executors.newFixedThreadPool(tableNames.length);
		for (int i=0; i<tableNames.length; i++) {			
			final String table = tableNames[i];			
			executor.submit(new Runnable() {			
				public void run() {
					try {
						LockCleaner.cleanLocks(conf, table);
					} catch(IOException e) {
						e.printStackTrace();
					}
				}
			});		
		}
	}
	
	public void deleteTable(String tableName) throws IOException {
		try{
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("Deleted table "+tableName);
		}catch(Exception e){}
			
	}
	
	public void deleteTables() throws IOException {					
		String tableNames[] = {"WAREHOUSE", "DISTRICT", "STOCK", "CUSTOMER", "ORDER", "NEW-ORDER", "ORDER-LINE", "ITEM"};
		for (int i=0; i<tableNames.length; i++) {			
			final String table = tableNames[i];
			deleteTable(table);
		}
			
	}	
	
	
	public void createRootTxn() throws IOException{
		TransactionManager tManager = new TransactionManager();
		TransactionHandler t = tManager.startTransaction(); //dummy transaction
		t.commit();
		System.out.println("created root transaction");
	}
	
	public TPCCSetup() throws IOException{
		conf = Config.getInstance().getHBaseConfig();
		admin = new HBaseAdmin(conf);		
	}
	
	
	public void setupDB(String option) throws Exception{
		calcRowCounts();
		if(option.equals("-all")) {
			//do everything
			System.out.println("Deleting existing TPCC tables");
			deleteTables();
			System.out.println("Creating TPCC tables");
			createTables();
			System.out.println("Populating database");
			populateAll(0,(int)NUM_WHOUSE);
			// System.out.println("Creating root transaction");
			// createRootTxn();
		}
		else if(option.contains("-c")) {
			String tableName = option.substring(2);
			if(tableName.equals("")) {
				System.out.println("Deleting existing TPCC tables");
				deleteTables();
				System.out.println("Creating TPCC tables");
				createTables();
			} else {				
				deleteTable(tableName);				
				createTable(tableName, tableSizes.get(tableName));
			}
		}
		else if(option.contains("-p")) {
			System.out.println("Populating database");
			String tableNameAndSpec = option.substring(2);
			int startPart=0;
			int endPart=(int)NUM_WHOUSE;
			String tableName = tableNameAndSpec;			
			if(tableNameAndSpec.contains(",")){
				StringTokenizer tok = new StringTokenizer(tableNameAndSpec,",");
				tableName=tok.nextToken();
				String partition = tok.nextToken();
				StringTokenizer tok2 = new StringTokenizer(partition,"-");
				startPart = Integer.parseInt(tok2.nextToken());
				endPart = Integer.parseInt(tok2.nextToken());
			}
			if(tableName.contains("&")) {
				StringTokenizer tok = new StringTokenizer(tableName, "&");
				String[] tableNames = new String[tok.countTokens()];
				for (int i=0; i<tableNames.length; i++)
					tableNames[i] = tok.nextToken();
				
				populate(startPart, endPart, tableNames);
			}
			else if(tableName.equals("ALL"))
				populateAll(startPart,endPart);
			else if(tableName.equals("ITEM"))
				populateItemTable();
			else if(tableName.equals("WAREHOUSE"))
				populateWarehouse(startPart, endPart);
			else if(tableName.equals("DISTRICT"))
				populateDistrict(startPart, endPart);
			else if(tableName.equals("STOCK"))
				populateStock(startPart,endPart);
			else if(tableName.equals("CUSTOMER"))
				populateCustomer(startPart,endPart);
			else if(tableName.equals("ORDER"))
				populateOrder(startPart,endPart);
			else if(tableName.equals("NEWORDER"))
				populateNewOrder(startPart,endPart);
			else
				System.out.println("Wrong Table Name. Chose between: ITEM/WAREHOUSE/DISTRICT/STOCK" +
						"/CUSTOMER/ORDER/NEWORDER/ALL");
			
		} 
		else if(option.equals("-t")) {
			System.out.println("Creating root transaction");
			createRootTxn();
		}
		else if(option.equals("-v")) {
			System.out.println("verifying DB..");
			verifyDB();
		} else if(option.equals("-u")) {
			System.out.println("cleaning locks");
			cleanLocks();
		}
		else {
			System.out.println("Wrong setup option. Chose between -c(create tables), -p(populate tables), -t(create root transaction), -v(verify DB)");			
		}
		System.exit(0);
	}

	public static void main(String args[]){
		try {
			Config config = new Config(args[0]);
			if(args.length > 1)
				NUM_WHOUSE = Integer.parseInt(args[1]);
			if (args.length > 2)
				numMachines = Integer.parseInt(args[2]);
			System.out.println("setting up database for number of warehouses:"+NUM_WHOUSE);	
			TPCCSetup setup = new TPCCSetup();			
			String[] tables = {"WAREHOUSE", "DISTRICT", "CUSTOMER", "ITEM", "ORDER", "NEW-ORDER", "ORDER-LINE", "STOCK", "HISTORY"};		
			//Batcher.startNewBatcher(Arrays.asList(tables));
			setup.setupDB(args[3]);
			
		} catch (Exception e) {
			e.printStackTrace();
			System.out.print("Error in setting up benchmark database");
			System.exit(1);
			
		}
		
		
	}
}
