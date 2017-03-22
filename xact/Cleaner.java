package xact;

import java.io.*;
import java.util.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

import util.*;

public class Cleaner{
	
	HTable counterTable;
	HTable cmtLogTable;
	DSGManagerInterface dsgManager;
	public Cleaner(){
		try{
			Config config = Config.getInstance();
			String counterTableName = config.getStringValue("counterTableName");
			String dsgTableName = config.getStringValue("dsgTableName");
			String cmtLogTableName = config.getStringValue("cmtLogTableName");
			counterTable = new HTable(config.getHBaseConfig(), counterTableName);
			//HTable dsgTable = new HTable(config.getHBaseConfig(), dsgTableName);
			cmtLogTable = new HTable(config.getHBaseConfig(), cmtLogTableName);
			dsgManager = new DSGManagerImpl(dsgTableName,cmtLogTable);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	private void advanceStableTS() {
		Config.getInstance().getLogger().logInfo("Advancing stable TS");
		try {
			Get g = new Get(Bytes.toBytes(TransactionManager.STS_CTR_ROWID));
			Result r = counterTable.get(g);
			long curSTS = Bytes.toLong(r.value());
			long old = curSTS;
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(curSTS+1));			
			scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("status"));
			scan.addColumn(Bytes.toBytes("d"), Bytes.toBytes("tid"));
			ResultScanner rs = cmtLogTable.getScanner(scan);			
			long ts = curSTS;
			Vector<Long> waitTS = new Vector<Long>();
			
			while((r=rs.next()) != null && !r.isEmpty()) {		
				int status = dsgManager.getXactStatus(Bytes.toInt(r.getValue(Bytes.toBytes("d"), Bytes.toBytes("tid"))), null);
				if (status < TransactionHandler.COMMIT_INCOMPLETE ) {
					long cleanupTS = Bytes.toLong(r.getRow());
					Config.getInstance().getLogger().logInfo(cleanupTS + "needs to cleaned up");
					waitTS.add(cleanupTS);
					rs.close();
					break;
				}
				else if(status == TransactionHandler.COMMIT_INCOMPLETE) {					
					dsgManager.doCommit(Bytes.toInt(r.getValue(Bytes.toBytes("d"), Bytes.toBytes("tid"))), 
							Bytes.toLong(r.getRow()));
				}
				ts = Bytes.toLong(r.getRow());
				//increment right-away to reflect it immediately
				
				Put p = new Put(Bytes.toBytes(TransactionManager.STS_CTR_ROWID));
				p.add(Bytes.toBytes("d"), Bytes.toBytes("val"),Bytes.toBytes(ts));
				boolean done = counterTable.checkAndPut(Bytes.toBytes(TransactionManager.STS_CTR_ROWID), Bytes.toBytes("d"), Bytes.toBytes("val"),
						Bytes.toBytes(curSTS), p);				
				if(done) {
					Config.getInstance().getLogger().logDebug("breached gap, advanced STS from "+curSTS+" to "+ts);
					curSTS = ts;
				}
			}
			long latest = curSTS;
			Config.getInstance().getLogger().logInfo("advanced STS from "+old+" to "+latest);
			waitAndCleanup(waitTS);
		}catch(Exception e){
			e.printStackTrace();			
		}		
	}
	
	private void waitAndCleanup(Vector<Long> waitTS) {
		try {
			Thread.sleep(500);
			for(Iterator<Long> it = waitTS.iterator(); it.hasNext();){
				long ts = it.next();
				Config.getInstance().getLogger().logInfo("Cleaning up "+ts);
				Get g = new Get(Bytes.toBytes(ts));
				Result r = cmtLogTable.get(g);
				int status = Bytes.toInt(r.getValue(Bytes.toBytes("d"), Bytes.toBytes("status")));
				int tid = Bytes.toInt(r.getValue(Bytes.toBytes("d"), Bytes.toBytes("tid")));
				Config.getInstance().getLogger().logInfo("aborting "+tid+" with "+ts+" known status:"+status);
				if(status < TransactionHandler.COMMIT_INCOMPLETE ) {
					if(dsgManager.changeXactStatus(tid, TransactionHandler.ABORT, status)) {
						Config.getInstance().getLogger().logInfo("aborted "+tid+" with "+ts);
						//dsgManager.logStatus(ts, TransactionHandler.ABORT);
						//abort cleanup..delete versions
					}else{
						int curStatus = dsgManager.getXactStatus(tid, null);
						Config.getInstance().getLogger().logInfo("can not abort "+tid+" with "+ts + "knownstatus:"+status+
								"curstatus:"+curStatus);
						
						
					}
				}else if(status == TransactionHandler.COMMIT_INCOMPLETE) {
					dsgManager.doCommit(tid,ts);
				}				
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private void pruneDSG(){
		try {
			dsgManager.pruneDSG();
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
	
	public static void main(String args[]){
		String configFile = args[0];
		Config config = new Config(configFile);
		
		Cleaner c1 = new Cleaner();
		try{
			while(true){
				c1.advanceStableTS();
				//c1.pruneDSG();
				Thread.sleep(200);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
