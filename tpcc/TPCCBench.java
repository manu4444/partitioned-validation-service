package tpcc;

import xact.*;
import util.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.security.SecureRandom;


public class TPCCBench extends Thread {
	TransactionManager tManager;	 
	Random rndWGen;
	Random rndDGen;
	Random rndCGen;	
	Random rndIGen;
	Random rndProb;
	Random rndMisc;
	int numWarehouses=1;
	static boolean stop = false;
	
	public TPCCBench(int numW, long seed){		
		rndProb = new Random();
		rndMisc = new Random();
		Random rndSource = new Random(seed);
		// create random id generators with diff seeds
		rndWGen = new Random(rndSource.nextLong());	
		rndDGen = new Random(rndSource.nextLong());
		rndCGen = new Random(rndSource.nextLong());
		rndIGen = new Random(rndSource.nextLong());
		tManager = new TransactionManager();
		numWarehouses = numW;
	}
	
	public void run() {
		while(!stop) {
			doTxn();
		}		
	}
	
	public void doTxn() {		
		int prob = rndProb.nextInt(100);
		try{
			if(prob<45) {
				doNewOrder();
			} else if(prob < 88) {
				doPayment();
			} else if(prob < 92) {
				doOrderStatus();
			}else if(prob < 96) {
				doDelivery();
			}else{
				doCreditCheck();
			}
			// Thread.sleep(20);
		}catch(Exception e) {
			e.printStackTrace();
		}		
	}
	
	public int makeStockEntry(long id) throws IOException {
		HTable table = new HTable(Config.getInstance().getHBaseConfig(), "STOCK");
		Put p = new Put(Bytes.toBytes(id),1);
		//p.add(Bytes.toBytes("d"), Bytes.toBytes("s_w_id"), Bytes.toBytes(i));
		int qty = (int) Math.random() * 91 + 10;
		p.add(Bytes.toBytes("d"), Bytes.toBytes("s_quantity"), Bytes.toBytes(qty));
		for(int k=0; k<TPCCSetup.DIST_SCALE;k++){
			byte[] dist = new byte[24];			
			String dist_num = "s_dist"+(k+1);
			p.add(Bytes.toBytes("d"), Bytes.toBytes(dist_num), dist);
		}
		p.add(Bytes.toBytes("d"), Bytes.toBytes("s_ytd"), Bytes.toBytes(0));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("d_order_cnt"), Bytes.toBytes(0));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("d_remove_cnt"), Bytes.toBytes(0));
		int length = (int) Math.random() * 26 + 25;
		byte[] data = new byte[length];		
		p.add(Bytes.toBytes("d"), Bytes.toBytes("s_data"), data);
		p.add(Bytes.toBytes("md"),Bytes.toBytes("tid"),Bytes.toBytes(1));
		p.add(Bytes.toBytes("md"),Bytes.toBytes("lock"),System.currentTimeMillis(),Bytes.toBytes(-1));		
		table.put(p);
		table.flushCommits();
		table.close();
		return qty;
	}
	
	public void makeCustomerEntry(long id) throws IOException {
		HTable table = new HTable(Config.getInstance().getHBaseConfig(), "CUSTOMER");
		Random rndSource = new Random();
		Put p = new Put(Bytes.toBytes(id),1);
		//p.add(Bytes.toBytes("d"), Bytes.toBytes("c_d_id"), Bytes.toBytes(j));
		//p.add(Bytes.toBytes("d"), Bytes.toBytes("c_w_id"), Bytes.toBytes(i));
		byte[] c_last = new byte[12];
		rndSource.nextBytes(c_last);
		p.add(Bytes.toBytes("d"), Bytes.toBytes("c_last"), c_last);
		p.add(Bytes.toBytes("d"), Bytes.toBytes("c_middle"), Bytes.toBytes("OE"));
		int length= rndSource.nextInt(9)+8;
		byte[] c_first = new byte[length];
		rndSource.nextBytes(c_first);
		p.add(Bytes.toBytes("d"), Bytes.toBytes("c_first"), c_first);
		length=rndSource.nextInt(11)+10;
		byte[] street1 = new byte[length];
		rndSource.nextBytes(street1);
		p.add(Bytes.toBytes("d"), Bytes.toBytes("c_street1"), street1);
		length=rndSource.nextInt(11)+10;
		byte[] street2 = new byte[length];
		rndSource.nextBytes(street2);
		p.add(Bytes.toBytes("d"), Bytes.toBytes("c_street2"), street2);
		length=rndSource.nextInt(11)+10;
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
		if(rndSource.nextInt(100) > 89)
			c_credit = "BC";
		p.add(Bytes.toBytes("d"), Bytes.toBytes("c_credit"), Bytes.toBytes(c_credit));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("c_credlimit"), Bytes.toBytes(50000));
		float disc= rndSource.nextFloat()*5;
		p.add(Bytes.toBytes("d"), Bytes.toBytes("c_discount"), Bytes.toBytes(disc));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("c_balance"), Bytes.toBytes(-10));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("c_ytd_pmt"), Bytes.toBytes(10.0));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("c_pmt_cnt"), Bytes.toBytes(1));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("c_delivery_cnt"), Bytes.toBytes(1));
		length=rndSource.nextInt(301)+200;
		byte[] data = new byte[length];
		rndSource.nextBytes(data);
		p.add(Bytes.toBytes("d"), Bytes.toBytes("c_data"), data);
		p.add(Bytes.toBytes("md"),Bytes.toBytes("tid"),Bytes.toBytes(1));
		p.add(Bytes.toBytes("md"),Bytes.toBytes("lock"),System.currentTimeMillis(),Bytes.toBytes(-1));
		table.put(p);
		table.flushCommits();
		table.close();
	}
	
	public void doNewOrder() throws IOException {		
		TransactionHandler t = tManager.startTransaction();
		try {
		long wid = rndWGen.nextInt(numWarehouses);
		long did = rndDGen.nextInt(TPCCSetup.DIST_SCALE);
		//Get g = new Get(Bytes.toBytes(wid));
		//g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("w_tax"));
		// Result res = t.get("WAREHOUSE", g);		
		// float wTax = Bytes.toFloat(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("w_tax")));		
		
		long cid = rndCGen.nextInt(TPCCSetup.CUST_SCALE);
		// System.out.println("did:"+did+" cid:"+cid);
		long custID = wid*TPCCSetup.DIST_SCALE * TPCCSetup.CUST_SCALE+ did* TPCCSetup.CUST_SCALE+ cid;
		//System.out.println("NewOrder: did:"+did+" cid:"+cid+" custid:"+custID);
		//System.out.println(custID);
		//g = new Get(Bytes.toBytes(custID));
		//g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("c_discount"));
		//g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("c_last"));
		//g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("c_credit"));
		//Result res = t.get("CUSTOMER", g);
		// float disc = Bytes.toFloat(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("c_discount")));
	
			
		long distID = wid* TPCCSetup.DIST_SCALE + did;
		Get g = new Get(Bytes.toBytes(distID));
		g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("d_next_oid"));	
		g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("d_tax"));
		Result res = t.get("DISTRICT", g);
		if (res.isEmpty() || !res.containsColumn(Bytes.toBytes("d"), Bytes.toBytes("d_tax")) || !res.containsColumn(Bytes.toBytes("d"), Bytes.toBytes("d_next_oid"))) {
			makeCustomerEntry(custID);
			res = t.get("DISTRICT", g);
		}
		float dTax = Bytes.toFloat(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("d_tax")));
		long nextOid = Bytes.toLong(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("d_next_oid")));		
		Put p = new Put(Bytes.toBytes(distID));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("d_next_oid"), Bytes.toBytes(nextOid+1));
		t.put("DISTRICT", p);
		
		long oid = nextOid;		
		long ordID = wid*TPCCSetup.DIST_SCALE*TPCCSetup.ORDER_SCALE + did * TPCCSetup.ORDER_SCALE + oid;		
		p = new Put(Bytes.toBytes(ordID));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("o_c_id"), Bytes.toBytes(custID));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("o_entry"), Bytes.toBytes(System.currentTimeMillis()));
		int olCnt = rndMisc.nextInt(11)+5;
		p.add(Bytes.toBytes("d"), Bytes.toBytes("o_ol_cnt"), Bytes.toBytes(olCnt));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("o_all_local"), Bytes.toBytes(1));
		t.put("ORDER", p);
		p = new Put(Bytes.toBytes(ordID));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("dummy"), Bytes.toBytes(1));
		t.put("NEW-ORDER", p);
		
		for(int olNum = 1 ; olNum <= olCnt; olNum++ ) {
			long olSupplyWid = wid;
			boolean local = true;
			long itemId = rndIGen.nextInt(TPCCSetup.ITEM_SIZE);
			int quantity = rndMisc.nextInt(10)+1;
						
			g = new Get(Bytes.toBytes(itemId));
			g.addColumn(Bytes.toBytes("d"),Bytes.toBytes("i_price"));
			g.addColumn(Bytes.toBytes("d"),Bytes.toBytes("i_name"));
			g.addColumn(Bytes.toBytes("d"),Bytes.toBytes("i_data"));
			res = t.get("ITEM",g);
			float price = Bytes.toFloat(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("i_price")));
							
			long stockId = wid*TPCCSetup.STOCK_SCALE + itemId;
			g = new Get(Bytes.toBytes(stockId));						
			g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("s_quantity"));
			res = t.get("STOCK",g);
			int stockQty;
			if (res == null || res.isEmpty() || res.value() == null) {
				stockQty = makeStockEntry(stockId);
			} else {
				// System.out.println("got result");
				stockQty = Bytes.toInt(res.value());
			}
			int newQty = 0;
			if(stockQty > quantity)
				newQty = stockQty - quantity;
			else
				newQty = stockQty - quantity + 91;

			p = new Put(Bytes.toBytes(stockId));
			p.add(Bytes.toBytes("d"), Bytes.toBytes("s_quantity"), Bytes.toBytes(newQty));
			t.put("STOCK",p);
					
			float amt = quantity * price;
			long olID = ordID * 100 + olNum;
			Put pLine = new Put(Bytes.toBytes(olID));
			pLine.add(Bytes.toBytes("d"), Bytes.toBytes("ol_i_id"), Bytes.toBytes(itemId));
			pLine.add(Bytes.toBytes("d"), Bytes.toBytes("ol_supply_wid"), Bytes.toBytes(wid));
			pLine.add(Bytes.toBytes("d"), Bytes.toBytes("ol_quantity"), Bytes.toBytes(quantity));
			pLine.add(Bytes.toBytes("d"), Bytes.toBytes("ol_amount"), Bytes.toBytes(amt));
			t.put("ORDER-LINE", p);				
		}
       	t.commit();	
		t.close();	
		} catch (NullPointerException ne) {
			//System.out.println(t.getSnapshotTS());
			t.commit();
			t.close();
		}
	}

	public void doPayment() throws IOException{
		long wid = rndWGen.nextInt(numWarehouses);
		long did = rndDGen.nextInt(TPCCSetup.DIST_SCALE);
		long cid = rndCGen.nextInt(TPCCSetup.CUST_SCALE);
		long custID = wid*TPCCSetup.DIST_SCALE * TPCCSetup.CUST_SCALE+ did* TPCCSetup.CUST_SCALE+ cid;
		//System.out.println("Payment: did:"+did+" cid:"+cid+" custid:"+custID);
		float hAmt = rndMisc.nextFloat() * 5000;

		TransactionHandler t = tManager.startTransaction();
		try {
		Put p = new Put(Bytes.toBytes(wid));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("w_ytd"),Bytes.toBytes(hAmt));
			
		//t.put("WAREHOUSE",p);
		
		long distID = wid* TPCCSetup.DIST_SCALE + did;
		p = new Put(Bytes.toBytes(distID));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("d_ytd"),Bytes.toBytes(hAmt));
		//t.put("DISTRICT",p);		
		
		Get g = new Get(Bytes.toBytes(custID));
		//System.out.println(custID);
		Result res = t.get("CUSTOMER",g);
		if (res == null || res.isEmpty()) {
			makeCustomerEntry(custID);
			res = t.get("CUSTOMER",g);
		}
		float custBalance = Bytes.toFloat(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("c_balance")));
		custBalance -= hAmt;
		float custPmt = Bytes.toFloat(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("c_ytd_pmt")));
		custPmt += hAmt;
		int pmtCnt = Bytes.toInt(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("c_pmt_cnt")));
		pmtCnt++;
		
		String credit = Bytes.toString(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("c_credit")));
		if(credit.equals("BC")) {
			byte data[] = res.getValue(Bytes.toBytes("d"), Bytes.toBytes("c_data"));
			String dataString = Bytes.toString(data);
			dataString += custID+" "+hAmt;
			p = new Put(Bytes.toBytes(custID));
			p.add(Bytes.toBytes("d"), Bytes.toBytes("c_data"), Bytes.toBytes(dataString));
			t.put("CUSTOMER", p);
		}else{
			p = new Put(Bytes.toBytes(custID));
			p.add(Bytes.toBytes("d"), Bytes.toBytes("c_balance"), Bytes.toBytes(custBalance));
			p.add(Bytes.toBytes("d"), Bytes.toBytes("c_ytd_pmt"), Bytes.toBytes(10.0));
			p.add(Bytes.toBytes("d"), Bytes.toBytes("c_pmt_cnt"), Bytes.toBytes(pmtCnt));
			t.put("CUSTOMER", p);
		}
		t.commit();
		t.close();
		} catch(NullPointerException ne){
			t.commit();
			t.close();
		}
	}
	
	public void injectFailure() throws IOException {
		
	}

	public void doOrderStatus() throws IOException{		
		int wid = rndWGen.nextInt(numWarehouses);
		int did = rndDGen.nextInt(TPCCSetup.DIST_SCALE);
		int cid = rndCGen.nextInt(TPCCSetup.CUST_SCALE);
		long custID = wid*TPCCSetup.DIST_SCALE * TPCCSetup.CUST_SCALE+ did* TPCCSetup.CUST_SCALE+ cid;
		long distID = wid* TPCCSetup.DIST_SCALE + did;
		//System.out.println("OrderStatus: did:"+did+" cid:"+cid+" custid:"+custID);
		TransactionHandler t = tManager.startTransaction();		
		try {
		long oid = rndMisc.nextLong();		
		long ordID = wid*TPCCSetup.DIST_SCALE*TPCCSetup.ORDER_SCALE + did * TPCCSetup.ORDER_SCALE + oid;
		Get g = new Get(Bytes.toBytes(ordID));
		Result res = t.get("ORDER", g);
		if (res.isEmpty()) {
			t.commit();
			t.close();
			return;
		}
		int olCnt = Bytes.toInt(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("o_ol_cnt")));
		for (int n=0;n<olCnt;n++)
		{
			long olId = ordID + n;
			g = new Get(Bytes.toBytes(olId));
			res = t.get("ORDER-LINE", g);
		}
		t.commit();
		t.close();
		} catch(NullPointerException ne) {
			t.commit();
			t.close();
		}
	}
	
	public void doDelivery() throws IOException {
		int wid = rndWGen.nextInt(numWarehouses);		
		HTable table = new HTable(Config.getInstance().getHBaseConfig(), "NEW-ORDER");
		HTable table2 = new HTable(Config.getInstance().getHBaseConfig(), "ORDER-LINE");
		for (int did=0; did < TPCCSetup.DIST_SCALE; did++) {
			
			long id = wid*TPCCSetup.DIST_SCALE*TPCCSetup.ORDER_SCALE + did*TPCCSetup.ORDER_SCALE;
			Scan scan = new Scan(Bytes.toBytes(id));
			ResultScanner rscan = table.getScanner(scan);
			Result res = rscan.next();
			if (res == null) {				
				return;
			}
			byte[] oid = res.getRow();
			TransactionHandler t = tManager.startTransaction();
			try {
			Get g = new Get(oid);
			res = t.get("NEW-ORDER", g);
			g = new Get(oid);
			g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("o_c_id"));
			g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("o_ol_cnt"));
			res = t.get("ORDER", g);
			if (res == null || res.isEmpty()) {
				t.commit();
				t.close();
				return;
			}
			long custID = Bytes.toLong(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("o_c_id")));
			int olCnt = Bytes.toInt(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("o_ol_cnt")));
			float totalAmt = 0;
			for (int n=0;n<olCnt;n++)
			{
				long ordID =Bytes.toLong(oid); 
				long olId = ordID * 100 + n;
				//System.out.println("olId:"+olId+ " ordID:"+ordID);
				g = new Get(Bytes.toBytes(olId));
				g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("ol_amount"));
				res = t.get("ORDER-LINE", g);
				if(!res.isEmpty()) {
					totalAmt += Bytes.toFloat(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("ol_amount")));
					Put p = new Put(Bytes.toBytes(olId));
					p.add(Bytes.toBytes("d"), Bytes.toBytes("ol_delivery"), Bytes.toBytes(System.currentTimeMillis()));
					t.put("ORDER-LINE", p);
				}
			}
			t.commit();
			t.close();
			} catch(NullPointerException ne) {
				t.commit();
				t.close();
			}
		}		
	}
	
	public void doStockLevel() throws IOException {
		
	}	
	
	public void doCreditCheck() throws IOException {
		int wid = rndWGen.nextInt(numWarehouses);
		int did = rndDGen.nextInt(TPCCSetup.DIST_SCALE);
		HTable table = new HTable(Config.getInstance().getHBaseConfig(), "NEW-ORDER");		
		long id = wid*TPCCSetup.DIST_SCALE*TPCCSetup.ORDER_SCALE+did*TPCCSetup.ORDER_SCALE;
		Scan scan = new Scan(Bytes.toBytes(id));
		ResultScanner rscan = table.getScanner(scan);
		Result res = rscan.next();
		byte[] oid = res.getRow();
		Get g = new Get(oid);
		TransactionHandler t = tManager.startTransaction();
		try {
		res = t.get("NEW-ORDER", g);
		g = new Get(oid);
		g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("o_c_id"));
		g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("o_ol_cnt"));
		res = t.get("ORDER", g);
		long custID = Bytes.toLong(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("o_c_id")));
		int olCnt = Bytes.toInt(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("o_ol_cnt")));
		float totalAmt = 0;
		for (int n=0;n<olCnt;n++)
		{
			long ordID =Bytes.toLong(oid); 
			long olId = ordID * 100 + n;
			//System.out.println("olId:"+olId+ " ordID:"+ordID);
			g = new Get(Bytes.toBytes(olId));
			g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("ol_amount"));
			res = t.get("ORDER-LINE", g);				
			if(!res.isEmpty()) {
				totalAmt += Bytes.toFloat(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("ol_amount")));
			}
		}
		
		g = new Get(Bytes.toBytes(custID));
		g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("c_balance"));
		g.addColumn(Bytes.toBytes("d"), Bytes.toBytes("c_credlimit"));
		res = t.get("CUSTOMER", g);
		
		if (res == null || res.isEmpty()) {
			makeCustomerEntry(custID);
			res = t.get("CUSTOMER", g);
		}
		
		float balance = Bytes.toFloat(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("c_balance")));
		float credLimit = Bytes.toFloat(res.getValue(Bytes.toBytes("d"), Bytes.toBytes("c_credlimit")));
		
		String credit = "";
		if(balance + totalAmt > credLimit){
			credit = "BC";
		}else{
			credit = "GC";
		}
		
		Put p = new Put(Bytes.toBytes(custID));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("c_credit"), Bytes.toBytes(credit));
		t.put("CUSTOMER", p);
		t.commit();
		t.close();
		}catch(NullPointerException ne) {
			t.commit();
			t.close();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		Config config = new Config(args[0]);
		int numWarehouses = Integer.parseInt(args[1]);
		int numThreads = 5;
		if(args.length >= 3)
			numThreads = Integer.parseInt(args[2]);
		
		ReportService service = new ReportService();
		if(args.length >=4) {
			service.port = Integer.parseInt(args[3]);	
		}		 
		service.start();
		long seed = System.currentTimeMillis();
		if(args.length >=5)
			seed = seed + Long.parseLong(args[4]);
		
		Random rndSource = new Random(seed);
		String[] tables = {"WAREHOUSE", "DISTRICT", "CUSTOMER", "ITEM", "ORDER", "NEW-ORDER", "ORDER-LINE", "STOCK"};		
		Batcher.startNewBatcher(Arrays.asList(tables));
		for(int i=0; i< numThreads; i++) {
			TPCCBench bench = new TPCCBench(numWarehouses, rndSource.nextLong());	//give different seed values to different threads
			bench.start();
		}		
		
		LoggerThread lgThread = new LoggerThread();
		lgThread.start();
	
	}
}

class LoggerThread extends Thread {	
	public static boolean stop = false;
	public void run() {
		System.out.println("Logger thread started..");
		int count = 0;
		
		while(!stop) {
			try {
				Thread.sleep(60*1000);
				StatsManager.logStats();
				System.out.println("Current Threads:"+ Thread.currentThread().activeCount());
				/*
				Thread[] list = new Thread[Thread.currentThread().activeCount()];
				Thread.currentThread().getThreadGroup().enumerate(list);
				for(int i=0; i< list.length; i++) {
					System.out.println();
					System.out.println(list[i].getClass());
					StackTraceElement[] st = list[i].getStackTrace();
					for (int j=0; j < st.length; j++) { 
						System.out.println(st[j].toString());
						if (j>=2) break;
					}
				}
				*/
				// StatsManager.reset();
				count++;
				if(count>=5){
					TPCCBench.stop = true;
					stop = true;
					Batcher.stopBatcher();
				}
					
			}catch(Exception e){}
		}
		// System.exit(0);		
	}
}

class ReportService extends Thread {
    public int port=50000;	
	public void run() {
		try {
			ServerSocket sSocket = new ServerSocket(port);
			while(true) {
				Socket socket = sSocket.accept();
				DataInputStream din = new DataInputStream(socket.getInputStream());
				int req = din.readInt();
				if ( req == 0) {
					System.out.println(" reporting stats to monitor");
					StatReport report = StatsManager.getStatReport();
					StatsManager.logStats();
					ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
					out.writeObject(report);
					out.close();
					socket.close();
					StatsManager.reset();
				} else {
					// stop
					TPCCBench.stop = true;
					LoggerThread.stop = true;
					Batcher.stopBatcher();
					try {
						
						socket.close();
						sSocket.close();
						break;
					} catch(Exception e){}
				}
			}
		}catch(IOException e){
			// stop
			TPCCBench.stop = true;
			LoggerThread.stop = true;
			Batcher.stopBatcher();
			e.printStackTrace();
		}
		
		
	}

}
