package xact;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;

import certifier.CertifierRequest;

import util.*;

/*
 * ChangeLog:
 * CH1 - Oct 17 2011 - Vinit Padhye - Changed according to transaction protocol version 4
 *       $XACT_HOME/Design/Snapshot-Isolation/design_docs/design_doc_Version4
 *       All changes identified through tag <CH1>   
 */

/**
 * @author padhye
 *
 */
public class TransactionHandler {
	//Transaction Status indicating its execution stages
	public static final int ACTIVE = 1;
	public static final int DSG_UPDATE = 2;
	public static final int VALIDATION = 3;	
	public static final int COMMIT_INCOMPLETE = 4;
	public static final int COMMIT_COMPLETE = 5;
	public static final int ABORT = 6;
	public static final int ABORT_CLEANED = 7;
	
	//abort reason
	public static final int NO_ABORT = 0;
	public static final int ABORT_SI = 1;
	public static final int ABORT_SER = 2;
	public static final int ABORT_OTHER = 3;
	
	private int tid;
	private long snapshotTS;
	private long commitTS;
	
	private TransactionManager txnManager;
	private DSGManagerInterface dsgManager;
	private Logger logger;
	
	private Hashtable<String,StorageTableInterface> storageTables; 
	
	private int status=0;
	private boolean written=false;
	
	boolean rdOnly = true;
	
	//performance statistics
	private long startTxn, startCmt;
	long delayDSG, delayVAL, delayCI, delayCC, delayExec, delayCmt;
	long delayDep, delayAdvanceSTS, delayLock, delayWait;
	long delayDSGTable;
	int abortReason = NO_ABORT;
	
	Batcher batcher;
	
	/**
	 * @param lockTID id of this transaction
	 * @param snapshot_ts start/snapshot timestamp
	 */
	public TransactionHandler(TransactionManager txnManager, int txnid, long snapshot_ts) throws IOException{
		this.snapshotTS = snapshot_ts;
		this.tid = txnid;
		
		this.txnManager = txnManager;
		// dsgManager = txnManager.getDSGManager();		
		storageTables = new Hashtable<String, StorageTableInterface>();		
		//add node in DSGTable
		startTxn = System.currentTimeMillis();
		long start = System.currentTimeMillis();
		// dsgManager.addXactNode(tid,snapshotTS);		
		long delay = System.currentTimeMillis();
		delayDSGTable = delay;
		status = ACTIVE;
		rdOnly = true;
		logger = Config.getInstance().getLogger();
		logger.logDebug("<TransactionHandler#T"+tid+"> Started with snapshot_ts:"+snapshotTS);
		StatsManager.reportNewTxn();
		batcher = txnManager.batcher;
	}
	
	protected void finalize() {
		close();
	}
	
	/** Reads the appropriate object version from the snapshot.
	 * @param tableName - name of the table to read from 
	 * @param get - Get object specifying the row and columns to read. @see org.apache.hadoop.hbase.client.Get
	 * @return Result object retrieved after reading from the snapshot
	 */
	public Result get(String tableName, Get get) throws IOException{
		if(storageTables.containsKey(tableName)){
			StorageTableInterface sTable = storageTables.get(tableName);
			return sTable.get(get);
		}else{
			StorageTableInterface sTable = new StorageTableImpl(tableName,tid,snapshotTS);
			storageTables.put(tableName, sTable);
			return sTable.get(get);
		}
	}
	
	/** Lazy: Buffers the writes until commit time
	 *  Eager: Writes immediately with tid as timestamp
	 * @param tableName - name of the table to write to
	 * @param put - Put object specifying the data to write. @see org.apache.hadoop.hbase.client.Put
	 * @throws IOException
	 */
	public void put(String tableName, Put put) throws IOException{
		if(storageTables.containsKey(tableName)){
			StorageTableInterface sTable = storageTables.get(tableName);
			sTable.put(put);
		}else{
			StorageTableInterface sTable = new StorageTableImpl(tableName,tid,snapshotTS);
			storageTables.put(tableName, sTable);
			sTable.put(put);
		}
		rdOnly = false;
	}
	
	public void put(String tableName, List<Put> puts) throws IOException{
		if(storageTables.containsKey(tableName)){
			StorageTableInterface sTable = storageTables.get(tableName);
			sTable.put(puts);
		}else{
			StorageTableInterface sTable = new StorageTableImpl(tableName,tid,snapshotTS);
			storageTables.put(tableName, sTable);
			sTable.put(puts);
		}
		rdOnly = false;
	}
	
	
	/** Performs commit of this transaction
	 * @return true if transaction successfully commits, false otherwise
	 */
	public boolean commit() throws IOException {
		startCmt = System.currentTimeMillis();
		delayExec = startCmt - startTxn;
		logger.logDebug("<TransactionHandler#T"+tid+"> Starting commit protocol");
		logger.logDebug("<TransactionHandler#T"+tid+"> Starting Validation stage");
        long startVal = System.currentTimeMillis();
        boolean done;
        /*
        boolean done = performValidation();
        delayVAL = System.currentTimeMillis() - startVal;
        if(!done) {
            logger.logDebug("<TransactionHandler#T"+tid+"> Validation failed");
            delayCmt = System.currentTimeMillis() - startCmt;           
            abort();
            return false;
        }
        */
		try{
			obtainCommitTS();
			// dsgManager.changeXactTS(tid, commitTS, status);
		}catch(IOException ioe){
			logger.logDebug("<TransactionHandler#T"+tid+"> Error in obatining commit timestamp: "+ioe.getMessage());
			return false;
		}
		//long startVal = System.currentTimeMillis();
		//logger.logDebug("<TransactionHandler#T"+tid+"> Requesting commit certification from certifier");
		//boolean done = getCertification();
		//if(!done){
			//logger.logDebug("<TransactionHandler#T"+tid+"> Certification failed");
			//return false;
		//}
		//delayVAL = System.currentTimeMillis() - startVal;
		logger.logDebug("<TransactionHandler#T"+tid+"> Starting CommitIncomplete stage");
		long startCI = System.currentTimeMillis();
		done = performCommitIncomplete();
		delayCI = System.currentTimeMillis() - startCI;
		if(!done){
			logger.logDebug("<TransactionHandler#T"+tid+"> CommitIncomplete failed");
			return false;
		}
		logger.logDebug("<TransactionHandler#T"+tid+"> Starting CommitComplete stage");
		long startCC = System.currentTimeMillis();
		performCommitComplete();
		delayCC = System.currentTimeMillis() - startCC;
		logger.logDebug("<TransactionHandler#T"+tid+"> committed with commit_ts:"+commitTS);
		StatsManager.reportStats(delayDSG, delayVAL, delayCI, delayCC, delayExec, delayCmt, 
				delayDep, delayAdvanceSTS, delayLock, delayWait,abortReason, rdOnly);
		return true;		
	}
	
	private boolean getCertification() {
		Hashtable<String, Pair<Collection<byte[]>, Collection<byte[]>>> rwSet= 
			new Hashtable<String, Pair<Collection<byte[]>, Collection<byte[]>>>();
		for (StorageTableInterface table: storageTables.values()){
			Vector<byte[] > readSet = new Vector<byte[]>();
			Vector<byte[] > writeSet= new Vector<byte[]>();
			for(Get g: table.getReadSet()){
				readSet.add(g.getRow());
			}
			for(Put p: table.getWriteSet()){
				writeSet.add(p.getRow());
			}
			Pair<Collection<byte[]>, Collection<byte[]>> rw = new Pair<Collection<byte[]>, Collection<byte[]>>();
			rw.setFirst(readSet);
			rw.setSecond(writeSet);
			rwSet.put(table.getTableName(), rw);
		}
		CertifierRequest request = new CertifierRequest(CertifierRequest.CERTIFY_BOTH, rwSet,snapshotTS,commitTS,tid);
		//CertifierRequest request = new CertifierRequest(CertifierRequest.CERTIFY_SI, rwSet,snapshotTS,commitTS,tid);
		try{			
			String host = Config.getInstance().getStringValue("certifierServerName");
			int port = Config.getInstance().getIntValue("certifierServerPort");
			Socket socket = new Socket(host,port);
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(request);
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			int status = in.readInt();
			this.abortReason = 	status;		
			if(status == NO_ABORT) {
				return true;
			}else{
				return false;
			}
		}catch(IOException ioe){
			logger.logDebug("<TransactionHandler#T"+tid+"> Error getting certification:"+ioe.getMessage());
			return false;
		}
	}
	/** Aborts this transaction
	 * 
	 */
	public void abort() throws IOException{
		logger.logDebug("<TransactionHandler#T"+tid+"> Transaction aborted");		
		// dsgManager.changeXactStatus(tid, ABORT, status);
		// dsgManager.logCmtStatus(commitTS, ABORT);
		if(commitTS > 0) {
			txnManager.advanceStableTS(commitTS);
		}
		
		//rollback all the tentative writes
		/*
		for(Enumeration<StorageTableInterface> en = storageTables.elements();en.hasMoreElements();){
			StorageTableInterface sTable = en.nextElement();
			sTable.rollbackWrites();
		}
		*/
				
		// dsgManager.changeXactStatus(tid, ABORT_CLEANED, 0);
		//dsgManager.deleteXactNode(tid);		
		StatsManager.reportStats(delayDSG, delayVAL, delayCI, delayCC, delayExec, delayCmt, 
				delayDep, delayAdvanceSTS, delayLock, delayWait,abortReason,rdOnly);
	}
	
	public void close() {
		//System.out.print("\n Thread Count before calling close:"+Thread.currentThread().activeCount());
		for (String key : storageTables.keySet()) {
			StorageTableInterface sTable = storageTables.get(key);
			sTable.close();
		}
		//System.out.print("Thread Count before calling close:"+Thread.currentThread().activeCount()+"\n");
	}
	
	/*
	 * Wait for the given status of transaction#tid, returns 1 if that transaction status is changed to the
	 * given status, 0 if the transaction is aborted or -1 if timeout
	 */
	private int waitForStatus(int tid, int status, int retries) throws IOException{
		if(retries==0)
			return -1;
		Long changeTime = new Long(0);
		int curStatus = dsgManager.getXactStatus(tid, changeTime);
		if(curStatus==-1){
			//node deleted i.e. aborted 
			return 0;
		}
		if(curStatus < status ){
			Config.getInstance().getLogger().logDebug("<TransactionHandler#T"+this.tid+"> waiting for status of T"+tid+ " for "+retries+" times");
			try{
				Thread.currentThread().sleep(100);				
			}
			catch(Exception e){
				e.printStackTrace();
			}
			return waitForStatus(tid, status,retries--);
		}
		else if(curStatus >= status && (curStatus != ABORT || curStatus != ABORT_CLEANED))
			return 1;
		else
			return 0;		
	}
		
	private boolean acquireLocks() throws Exception {
		HashMap<String, RWSet> rwSetMap = new HashMap<String, RWSet>();
		int workSize = 0;
		for(String key : storageTables.keySet()) {
			StorageTableInterface sTable = storageTables.get(key);
			List<Put> puts = sTable.getWriteSet();
			Vector<Long> writeSet = new Vector<Long>(); 
			for (Put p : puts) 
				writeSet.add(Bytes.toLong(p.getRow()));			
			
			RWSet rwSet = new RWSet(snapshotTS, tid, writeSet);
			rwSetMap.put(key, rwSet);
			workSize += writeSet.size();
		}
		if (workSize > 0) {		// if there is any locks to be acquired, send it to batcher
			batcher.reportWork(rwSetMap, workSize);					
			return batcher.fetchResult(tid);
		} else {
			return true;
		}
	}
	
	/*
	 * Performs DSGUpdate stage of the commit protocol. Returns true if this stage is successfully
	 * completed and that transaction can proceed further. Returns false if ww-conflict is detected and
	 * the conflicting transaction commits, indicating that this transaction should abort.  
	 */
	private boolean performDSGUpdate(){
		status = DSG_UPDATE;		
		//return status of this stage, false indicates transaction would abort, true indicates transaction can proceed further
		boolean ret = true;		
		try { 
			//1. change status to DSGUpdate 
			dsgManager.changeXactStatus(tid, DSG_UPDATE, ACTIVE);
			logger.logDebug("<TransactionHandler#T"+tid+"> Changed status to DSG_UPDATE");		
			//2. mark writes and collect dependency information			
			//record writeset in DSGTable first
			byte[] writeSet = getWriteSet();
			//dsgManager.addWriteSet(tid, writeSet);
			Vector<Dependency> depList = new Vector<Dependency>();
			written = true;
			logger.logDebug("<TransactionHandler#T"+tid+"> Locking writes for transaction");
			long startLock = System.currentTimeMillis();
			for(Enumeration<StorageTableInterface> en = storageTables.elements();en.hasMoreElements();){
				StorageTableInterface sTable = en.nextElement();
				boolean rLocked = sTable.lockReads();
				if(!rLocked) {
					abortReason = ABORT_SER;
					logger.logDebug("<TransactionHandler#T"+tid+"> Aborted due to serializabilty conflict");
					abort();	
					return false;
				}
				boolean wLocked = sTable.lockWrites();			
				if(!wLocked) {
					int conflictTID = sTable.getWWConflict();
					logger.logDebug("<TransactionHandler#T"+tid+"> Conflict found with transaction#T"+conflictTID);
					int status = dsgManager.getXactStatus(tid, null);						
					if(conflictTID < tid){
						long startWait = System.currentTimeMillis();
						int waitStatus = waitForStatus(conflictTID, COMMIT_INCOMPLETE,4);
						delayWait = System.currentTimeMillis() - startWait;
						if( waitStatus == 1){
							//waitForStatus returned 1 i.e. the conflicting transaction has committed, so abort
							abortReason = ABORT_SI;
							logger.logDebug("<TransactionHandler#T"+tid+"> Aborted due to SI conflict");
							abort();	
							return false;
						}else if( waitStatus == -1){
							dsgManager.changeXactStatus(conflictTID, ABORT, 0);
						}
					} else{
						//conflicting transaction older than me, abort
						abortReason = ABORT_SI;
						logger.logDebug("<TransactionHandler#T"+tid+"> Aborted due to SI conflict");
						abort();	
						return false;
					}					
				}					
			}
			delayLock = System.currentTimeMillis() - startLock;
			
			/*
			logger.logDebug("<TransactionHandler#T"+tid+"> Finding dependencies for transaction");
			long startDep = System.currentTimeMillis();
			for(Enumeration<StorageTableInterface> en = storageTables.elements();en.hasMoreElements();){
				StorageTableInterface sTable = en.nextElement();
				Vector<Dependency> list = sTable.getDependency();
				for (Iterator<Dependency> it = list.iterator(); it.hasNext();){
					Dependency dep = it.next(); 
					if(!depList.contains(dep)){
						depList.add(dep);
					}
				}
			}			
			//3. insert dependency edges in DSGTable
			logger.logDebug("<TransactionHandler#T"+tid+"> Dependency List:"+depList);
			logger.logDebug("<TransactionHandler#T"+tid+"> Adding dependency for transaction");
			dsgManager.addDependency(depList);
			delayDep = System.currentTimeMillis() - startDep;
			*/			
		}catch(Exception e){
			ret = false;
			e.printStackTrace();
		}
		return ret;		
	}
	
	/*
	 * Performs the Validation stage of the commit protocol.
	 */
	private boolean performValidation(){
		status = VALIDATION;
		boolean ret = true;
		Logger logger = Config.getInstance().getLogger();
		try {
			//obtainCommitTS();
			// ret = dsgManager.changeXactStatus(tid, VALIDATION, ACTIVE); //change status			
			if(ret == true){
				//dsgManager.logStatus(commitTS, VALIDATION);
				//dsgManager.changeXactTS(tid, commitTS, status);
				logger.logDebug("<TransactionHandler#T"+tid+"> Performing VALIDATION");
				
				// acquire locks
				try {					
					ret = acquireLocks();					
				}catch(Exception e){
					e.printStackTrace();
					return false;
				}
				if(ret==false){
					abortReason = ABORT_SI;
					logger.logDebug("<TransactionHandler#T"+tid+"> Aborted due to SI conflict");					
					return false;
				}				
			}else{
				abortReason = ABORT_OTHER;
				logger.logDebug("<TransactionHandler#T"+tid+"> Transaction aborted by other");
				abort();
				ret = false;
			}
		}catch(Exception e){
			e.printStackTrace();
			ret = false;
		}
		return ret;
	}
	
	
	private boolean performCommitIncomplete(){
		status = COMMIT_INCOMPLETE;		
		Logger logger = Config.getInstance().getLogger();		
		try{			  
			// dsgManager.changeXactStatus(tid, COMMIT_INCOMPLETE, VALIDATION);
			logger.logDebug("<TransactionHandler#T"+tid+"> Changed status to CommitIncomplete");			
			//commit all the writes			
			if(!rdOnly) {
				logger.logDebug("<TransactionHandler#T"+tid+"> Committing writes: insert ts->tid mapping");				
				for(Enumeration<StorageTableInterface> en = storageTables.elements();en.hasMoreElements();){
					StorageTableInterface sTable = en.nextElement();
					sTable.setCommitTS(commitTS);
					sTable.commitWrites();				
				}
			}				
			logger.logDebug("<TransactionHandler#T"+tid+"> Transaction committed successfully");
			delayCmt = System.currentTimeMillis() - startCmt;				
			return true;			
		}catch(IOException ioe) {
			ioe.printStackTrace();
			return false;
		}
	}
	
	private void performCommitComplete() throws IOException{
		status = COMMIT_COMPLETE;
		// dsgManager.changeXactStatus(tid, COMMIT_COMPLETE, COMMIT_INCOMPLETE);		
		// dsgManager.logCmtStatus(commitTS, COMMIT_COMPLETE);
		logger.logDebug("<TransactionHandler#T"+tid+"> Advancing STS");
		long startAdvSTS = System.currentTimeMillis();
		txnManager.advanceStableTS(commitTS);
		delayAdvanceSTS = System.currentTimeMillis() - startAdvSTS;
		//txnManager.advanceCompletedTS(commitTS);		
	}
	
	private byte[] getWriteSet() throws IOException{		 
		//< tableName, Vector< rowid, Vector <fam+qual> > >
		Hashtable<String,Vector<byte[]>> writeSet = new Hashtable<String,Vector<byte[]>>();			
		for(Enumeration<StorageTableInterface> en = storageTables.elements();en.hasMoreElements();){
			StorageTableInterface sTable = en.nextElement();			
			Vector<Put> wset = sTable.getWriteSet();
			Vector<byte[]> rowList = new Vector<byte[]>();
			for(Iterator<Put> it = wset.iterator(); it.hasNext();){
				Put p = it.next();
				byte[] row = p.getRow();
				if(!rowList.contains(row))
					rowList.add(row);
			}
			writeSet.put(sTable.getTableName(), rowList);
		}
		ByteArrayOutputStream bs = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(bs);
		os.writeObject(writeSet);
		return bs.toByteArray();		
	}


	public int getTid() {
		return tid;
	}


	public long getSnapshotTS() {
		return snapshotTS;
	}


	public long getCommitTS() {
		return commitTS;
	}
	
	public void obtainCommitTS() throws IOException{
		commitTS = txnManager.getCommitTimestamp();			
		// dsgManager.changeXactTS(tid, commitTS,status);
		logger.logDebug("<TransactionHandler#T"+tid+"> Obtained commit_ts: "+commitTS);
	}
}
