/**
 * 
 */
package xact;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.*;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

import util.*;

/**
 * @author padhye
 *
 */

public class DSGManagerImpl implements DSGManagerInterface {
	
	
	private HTable cmtLogTable; //<ts->status>
	//private HTable precmtTable; //<ts->status>		
	//private HTable activeTable;  //<tid-> startts>
	private HTable committingTable;
	
	
	Logger logger;

	public DSGManagerImpl(String dsgTableName, HTable cmtLogTable) throws IOException{		
		this.cmtLogTable = cmtLogTable;
		//precmtTable = new HTable(Config.getInstance().getHBaseConfig(), "PRECMT");
		logger = Config.getInstance().getLogger();				
		//activeTable = new HTable(Config.getInstance().getHBaseConfig(), "ACTIVE");
		committingTable = new HTable(Config.getInstance().getHBaseConfig(), "COMMITTING");		
	}
	
	
	public void addDependency(Vector<Dependency> depList) throws IOException {				
		LinkedList<Put> putList = new LinkedList<Put>();  
		for(Iterator<Dependency> it=depList.iterator(); it.hasNext();){
			Dependency dep = it.next();
			Put p = new Put(Bytes.toBytes(dep.getFromTID()));
			p.add(Bytes.toBytes("d"),Bytes.toBytes(OUT_EDGE_COLNAME),Bytes.toBytes(dep.getToTID()));			
			committingTable.put(p);			
			//dsgTable.incrementColumnValue(Bytes.toBytes(dep.getToTID()), 
					//Bytes.toBytes("d"),Bytes.toBytes("i-count"), 1);
		}		
	}

	
	public boolean addXactNode(int tid, long startTS) throws IOException{
		Put p = new Put(Bytes.toBytes(tid));
		p.add(Bytes.toBytes("d"),Bytes.toBytes(START_TS_COLNAME),Bytes.toBytes(startTS));
		//p.add(Bytes.toBytes("d"),Bytes.toBytes(STATUS_COLNAME),Bytes.toBytes(TransactionHandler.ACTIVE));
		Get g = new Get(Bytes.toBytes(tid));		
		//boolean exists = committingTable.exists(g);
		//if(!exists) {
			boolean done = committingTable.checkAndPut(p.getRow(),Bytes.toBytes("d"),Bytes.toBytes(STATUS_COLNAME),null,p);		
			return done;
		//}else{
			//return false;
		//}
	}


	public boolean changeXactStatus(int tid, int newStatus, int oldStatus) throws IOException{
		Put p = new Put(Bytes.toBytes(tid));
		p.add(Bytes.toBytes("d"),Bytes.toBytes(STATUS_COLNAME),Bytes.toBytes(newStatus));
		//change status is conditional make sure the status
		boolean ret = true;		
		if( newStatus!= TransactionHandler.ABORT_CLEANED && newStatus!=TransactionHandler.COMMIT_COMPLETE && 
				newStatus!=TransactionHandler.DSG_UPDATE){
			//Config.getInstance().getLogger().logDebug("Changing status for "+tid+" as "+newStatus);			
			ret = committingTable.checkAndPut(Bytes.toBytes(tid),Bytes.toBytes("d"),Bytes.toBytes(STATUS_COLNAME),
				Bytes.toBytes(oldStatus), p);			
		} 
		else{			
			committingTable.put(p);			
			//Config.getInstance().getLogger().logDebug("Inserting node  "+tid+" status "+newStatus);
		}
		//if(newStatus==TransactionHandler.DSG_UPDATE){
			//Delete d = new Delete(Bytes.toBytes(tid));
			//activeTable.delete(d);
		//}	
		
		return ret;
	}
	
	public void logCmtStatus(long commitTS, int status) throws IOException{
		Put p = new Put(Bytes.toBytes(commitTS));
		p.add(Bytes.toBytes("d"),Bytes.toBytes(STATUS_COLNAME),Bytes.toBytes(status));
		cmtLogTable.put(p);
		
		//Delete d = new Delete(Bytes.toBytes(commitTS));
		//precmtTable.delete(d);
	}
	
	
	public void changeXactTS(int tid, long commitTS, int status) throws IOException {		
		Put p = new Put(Bytes.toBytes(tid));
		p.add(Bytes.toBytes("d"),Bytes.toBytes(CMT_TS_COLNAME),Bytes.toBytes(commitTS));		
		committingTable.put(p);
		
		//Put p2 = new Put(Bytes.toBytes(commitTS));
		//p2.add(Bytes.toBytes("d"),Bytes.toBytes("tid"),Bytes.toBytes(tid));		
		//precmtTable.put(p2);		
		
		
	}

	public void addWriteSet(int tid, byte[] writeSet) throws IOException {		
		Put p = new Put(Bytes.toBytes(tid));
		p.add(Bytes.toBytes("d"),Bytes.toBytes(WSET_COLNAME),writeSet);		
		committingTable.put(p);
	}

	public byte[] getWriteSet(int tid) throws IOException {		
		Get g = new Get(Bytes.toBytes(tid));
		g.addColumn(Bytes.toBytes("d"),Bytes.toBytes(WSET_COLNAME));		
		Result r = committingTable.get(g);
		return r.value();
	}

	public boolean checkCycle(int startTid) throws IOException{
		//perform DFS starting at startTid, cycle is found if any path reaches back to start node
		//if all the nodes on a cycle path are committed, then abort, else wait for their status, 
		//there may be more than once cycles, find all the cycles
		logger.logDebug("<DSGManager#T"+startTid+"> Checking cycle..");
		Stack<Dependency> nodeStack = new Stack<Dependency>();
		Vector<Integer> visitedNodes = new Vector<Integer>();
		//there may be more than one node to wait on so find all of them in one DFS traversal
		//to avoid traversing again
		Vector<Pair<Integer,Long>> waitNodes = new Vector<Pair<Integer,Long>>();	//list of nodes to wait on	
		
		Object obj[] = getOutEdges(startTid);				
		Vector<Integer> outEdges = (Vector<Integer>) obj[1];
		for(Iterator<Integer> it=outEdges.iterator();it.hasNext();) {
			nodeStack.add(new Dependency(startTid,it.next()));
		}	
		visitedNodes.add(startTid);
		while(!nodeStack.isEmpty()){
			Dependency dep = nodeStack.pop();
			logger.logDebug("<DSGManager#T"+startTid+"> visting node:"+dep);
			int tid = dep.getToTID();
			if(tid==startTid) {	//found edge back to start node..cycle found
				logger.logDebug("<DSGManager> Cycle found for transaction#"+startTid);
				logger.logDebug("<DSGManager#T"+startTid+"> Number of nodes traversed:"+visitedNodes.size());
				return false;											
			} 
			if(visitedNodes.contains(tid))
				continue;			
			
			visitedNodes.add(tid);		
			if(tid!=startTid) {				 
				obj = getOutEdges(tid);
				int status = (Integer) obj[0];				
				outEdges = (Vector<Integer>) obj[1];
				if(status < TransactionHandler.VALIDATION || status > TransactionHandler.COMMIT_COMPLETE) {
					logger.logDebug("<DSGManager#T"+startTid+"> Ignoring node "+tid);
					continue;	//ignore nodes which are aborted or not yet reached validation stage  
				}								
				for(Iterator<Integer> it=outEdges.iterator();it.hasNext();) {
					int id = it.next();					
					nodeStack.push(new Dependency(tid,id));										
				}				
			}
		}			
		logger.logDebug("<DSGManager#T"+startTid+"> Number of nodes traversed:"+visitedNodes.size());
		return true;
	}
	
	/*
	 * Wait for the given transaction's status, returns 0 if that transaction status is changed to the
	 * given status, -1 if the transaction is aborted, if timeout then returns its current status 
	 */
	private int waitForStatus(int tid, int status, int retries) throws IOException{		
		int curStatus = getXactStatus(tid, null);
		if(curStatus==-1){
			//node deleted i.e. it was aborted
			return -1;
		}
			
		if(curStatus < status ){			
			try{
				Thread.currentThread().sleep(120);				
			}
			catch(InterruptedException e){
				e.printStackTrace();				
			}
			retries--;
			if(retries==0)
				return curStatus;
			else
				return waitForStatus(tid, status,retries);
		}
		else if(curStatus == status)
			return 0;
		else
			return -1;
		
		
	}

	private Object[] getOutEdges(int tid) throws IOException{
		Vector<Integer> outEdges = new Vector<Integer>();
		Object obj[] = new Object[3];
		Get g = new Get(Bytes.toBytes(tid));
		g.addColumn(Bytes.toBytes("d"),Bytes.toBytes(OUT_EDGE_COLNAME));		
		g.setMaxVersions();		
		g.addColumn(Bytes.toBytes("d"),Bytes.toBytes(STATUS_COLNAME));		
		Result r = committingTable.get(g);
		
		if(r.isEmpty()) {
			obj[0] = TransactionHandler.ABORT_CLEANED;
			return obj;
		}		
		
		int status = Bytes.toInt(r.getValue(Bytes.toBytes("d"), Bytes.toBytes(STATUS_COLNAME)));
		
		for(Iterator<KeyValue> it = r.getColumn(Bytes.toBytes("d"),Bytes.toBytes(OUT_EDGE_COLNAME)).iterator(); it.hasNext();){
			int id = Bytes.toInt(it.next().getValue());
			outEdges.add(id);
		}		
		obj[0] = status;		
		obj[2] = outEdges;
		return obj;
	}

	public void deleteXactNode(int tid) throws IOException{		
		Delete d = new Delete(Bytes.toBytes(tid));		
		committingTable.delete(d);
	}

	/* (non-Javadoc)
	 * @see xact.DSGManagerInterface#doCommit(int)
	 */
	@Override
	public void doCommit(int tid, long ts) throws IOException {
		Get g = new Get(Bytes.toBytes(tid));
		//g.addColumn(Bytes.toBytes("d"),Bytes.toBytes(CMT_TS_COLNAME));
		g.addColumn(Bytes.toBytes("d"),Bytes.toBytes(WSET_COLNAME));		
		Result r = committingTable.get(g);
		//long ts = Bytes.toLong(r.getValue(Bytes.toBytes("d"),Bytes.toBytes(CMT_TS_COLNAME)));
		if(r.containsColumn(Bytes.toBytes("d"),Bytes.toBytes(WSET_COLNAME))){
			byte[] ws = r.getValue(Bytes.toBytes("d"),Bytes.toBytes(WSET_COLNAME));
			ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(ws));
			try{
				Hashtable<String,Vector<byte[]>> writeSet = (Hashtable<String,Vector<byte[]>>) is.readObject();
				for(Enumeration<String> en = writeSet.keys(); en.hasMoreElements();){
					String tableName = en.nextElement();
					StorageTableInterface sTable = new StorageTableImpl(tableName);
					Vector<byte[]> rowList = writeSet.get(tableName);
					for(Iterator<byte[]> it=rowList.iterator();it.hasNext();){
						sTable.commitVersion(it.next(), ts);
					}
				}
			}catch(ClassNotFoundException e){
				e.printStackTrace();
				logger.logWarning("<DSGManager>Error in reading writeset of transaction#"+tid);
				throw new IOException("Error in reading writeset of transaction"); 
			}
		}
		changeXactStatus(tid, TransactionHandler.COMMIT_COMPLETE, TransactionHandler.COMMIT_INCOMPLETE);
		Put p = new Put(Bytes.toBytes(ts));
		p.add(Bytes.toBytes("d"),Bytes.toBytes("status"),Bytes.toBytes(TransactionHandler.COMMIT_COMPLETE));
		cmtLogTable.put(p);
	}

	public void doAbort(int tid, long ts) throws IOException{
		Get g = new Get(Bytes.toBytes(tid));
		//g.addColumn(Bytes.toBytes("d"),Bytes.toBytes(CMT_TS_COLNAME));
		g.addColumn(Bytes.toBytes("d"),Bytes.toBytes(WSET_COLNAME));		
		Result r = committingTable.get(g);
		//long ts = Bytes.toLong(r.getValue(Bytes.toBytes("d"),Bytes.toBytes(CMT_TS_COLNAME)));
		if(r.containsColumn(Bytes.toBytes("d"),Bytes.toBytes(WSET_COLNAME))){
			byte[] ws = r.getValue(Bytes.toBytes("d"),Bytes.toBytes(WSET_COLNAME));
			ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(ws));
			try{
				Hashtable<String,Vector<byte[]>> writeSet = (Hashtable<String,Vector<byte[]>>) is.readObject();
				for(Enumeration<String> en = writeSet.keys(); en.hasMoreElements();){
					String tableName = en.nextElement();
					StorageTableInterface sTable = new StorageTableImpl(tableName);
					Vector<byte[]> rowList = writeSet.get(tableName);
					for(Iterator<byte[]> it=rowList.iterator();it.hasNext();){
						sTable.deleteVersion(it.next(), ts);
					}
				}
			}catch(Exception e){
				logger.logWarning("<DSGManager>Error in reading writeset of transaction#"+tid);				
			}
		}
		Put p = new Put(Bytes.toBytes(ts));
		p.add(Bytes.toBytes("d"),Bytes.toBytes("status"),Bytes.toBytes(TransactionHandler.ABORT_CLEANED));
		cmtLogTable.put(p);
	}

	
	public void pruneDSG() throws IOException{
		/*
		Config.getInstance().getLogger().logInfo("Prunning DSG");
		//Vector<Integer> unreachableNodes = new Vector<Integer>();
		try{
			HTable dsgTable = getTable(tid);		
			ResultScanner rsDSG = dsgTable.getScanner(Bytes.toBytes("d"));
			Result r;
			long oldestActive = Long.MAX_VALUE;			
			while( (r = rsDSG.next()) != null) {
				int status = Bytes.toInt(r.getValue(Bytes.toBytes("d"), Bytes.toBytes(STATUS_COLNAME)));
				if(status == TransactionHandler.ACTIVE || status == TransactionHandler.DSG_UPDATE) {					
					oldestActive = Bytes.toLong(r.getValue(Bytes.toBytes("d"), Bytes.toBytes(START_TS_COLNAME)));
					//System.out.println(" oldest active: "+oldestActive+ " status:"+status);
					break;					
				}				
			}
			
			rsDSG.close();
			Scan scan = new Scan(Bytes.toBytes(oldestActive));
			ResultScanner rsCMT = cmtLogTable.getScanner(scan);
			Vector<Integer> reachableNodes = new Vector<Integer>();
			while((r = rsCMT.next()) != null){
				int tid = Bytes.toInt(r.getValue(Bytes.toBytes("d"), Bytes.toBytes("tid")));
				reachableNodes.add(tid);
				//Config.getInstance().getLogger().logInfo("Reachable node "+tid);
				Object[] ret= getOutEdges(tid);
				Vector<Integer> outEdges = (Vector<Integer>)ret[2];
				while(outEdges != null && outEdges.size() > 0) {
					int nodeTid = outEdges.remove(0);
					if(!reachableNodes.contains(nodeTid)){
						//Config.getInstance().getLogger().logInfo("Reachable node "+nodeTid);
						reachableNodes.add(nodeTid);							
						ret= getOutEdges(tid);
						Vector<Integer> edges = (Vector<Integer>)ret[2];
						outEdges.addAll(edges);
					}
				}
			}
			
			Scan scan2 = new Scan();
			//scan2.setStartRow(Bytes.toBytes(1));
			scan2.setStopRow(Bytes.toBytes(oldestActive));
			ResultScanner rsCMT2 = cmtLogTable.getScanner(scan2);
			LinkedList deleteListDSG = new LinkedList<Integer>();
			LinkedList deleteListCMT = new LinkedList<Integer>();
			while((r = rsCMT2.next()) != null){
				//System.out.println("Am i getting here");
				if(r.isEmpty())
					break;
				if(!r.containsColumn(Bytes.toBytes("d"), Bytes.toBytes("tid"))){
					System.out.println(Bytes.toLong(r.getRow()));
					continue;
				}
				int tid = Bytes.toInt(r.getValue(Bytes.toBytes("d"), Bytes.toBytes("tid")));				
				if(!reachableNodes.contains(tid))  {
					//Config.getInstance().getLogger().logInfo("Unreachable node "+tid);
					//unreachableNodes.add(tid);
					Delete dDSG = new Delete(Bytes.toBytes(tid));
					Delete dCMT = new Delete(r.getRow());
					deleteListDSG.add(dDSG);
					deleteListCMT.add(dCMT);
				}else{
					//System.out.println(tid+" reachable");
				}
			}
			Config.getInstance().getLogger().logInfo("deleting unreachable nodes:"+deleteListDSG.size());
			dsgTable.delete(deleteListDSG);
			cmtLogTable.delete(deleteListCMT);
		
		}catch(IOException ioe){
			ioe.printStackTrace();
			
		}	
		*/
	}


	public int getXactStatus(int tid, Long changeTime) throws IOException{
		Get g = new Get(Bytes.toBytes(tid));
		g.addColumn(Bytes.toBytes("d"),Bytes.toBytes(STATUS_COLNAME));		
		Result r = committingTable.get(g);
		if(r==null || r.isEmpty())
			return -1;
		else
			return Bytes.toInt(r.value());
	}

	public int getXactTID(long ts) throws IOException{
		Get g = new Get(Bytes.toBytes(ts));
		g.addColumn(Bytes.toBytes("d"),Bytes.toBytes("tid"));
		Result r = cmtLogTable.get(g);
		if(r!=null && !r.isEmpty())
			return Bytes.toInt(r.value());
		else {	//txn with this ts not yet recorded its entry
			try {
				Thread.sleep(5); //wait for it to make entry
				r = cmtLogTable.get(g);
				if(r!=null && !r.isEmpty())
					return Bytes.toInt(r.value());
				else 
					return -1;	//timeout
			}catch(Exception e){return -1;}			
		}
	}
	

	
	
	

}
