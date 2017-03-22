package xact;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.client.coprocessor.*;

import util.*;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class Batcher extends Thread {
	static final long BATCHING_DELAY = 10;
	static final int BATCH_SIZE = 50;
	
	HashMap<String, HTable> tablePool;		// pool of HTable instances	 
	HashMap<String, List<RWSet>> workMap;	// list of RWsets to be given to each table
	HashMap<String, Pair<Long, Long>> rangeMap;
	HashMap<String, byte[][]> regionKeysMap;
	
	HashMap<Integer, Boolean> decisionMap;	// tid->commit/abort decision
	HashMap<Integer, Boolean> resultMap;	// tid->commit/abort decision
	HashMap<String, List<Long>> lockedItemsMap; //tablename->locked-items
	HashMap<String, HashMap<Integer, List<Long>>> lockResultMap;	// tablename->[tid->locked-items] .. temporary map to hold items locked for a given transaction
	
	ExecutorService	executor;
	
	boolean currentBatchDone = true;	//if we are done receiving callbacks for current batch
	int currentBatchCount = 0;		//no.of tables in current batch
	int doneCount = 0;	
	int workSize = 0;
	
	boolean stop = false;
	
	static Batcher batcher= null;
	
	Logger logger;	 
	
	protected Batcher(List<String> tableNames) throws IOException {
		tablePool = new HashMap<String, HTable>();
		rangeMap = new HashMap<String, Pair<Long, Long>>();
		regionKeysMap = new HashMap<String, byte[][]>();
		for (String tableName : tableNames) {
			HTable table = new HTable(Config.getInstance().getHBaseConfig(),tableName);
			tablePool.put(tableName, table);			
			byte[][] regionKeys = table.getStartKeys();
			regionKeysMap.put(tableName, regionKeys);
		}
		workMap = new HashMap<String, List<RWSet>>();
		lockResultMap = new HashMap<String, HashMap<Integer,List<Long>>>();
		decisionMap = new HashMap<Integer, Boolean>();
		resultMap = new HashMap<Integer, Boolean>();
		lockedItemsMap = new HashMap<String, List<Long>>();	
		logger = Config.getInstance().getLogger(); 
		executor = Executors.newCachedThreadPool();
	}	
	
	public static void startNewBatcher(List<String> tableNames) throws IOException {
		if (batcher != null) {
			System.out.println("A batcher is already started..Don't start multiple batchers");
			return;
		}
		batcher = new Batcher(tableNames);
		batcher.start();
	}
	
	public static void stopBatcher() {
		if (batcher == null) {
			System.out.println("Batcher not started yet..");
			return;
		}
		batcher.stop = true;
	}
	
	public static Batcher getBatcher() {
		if (batcher == null) {
			System.out.println("Batcher not started yet..exiting");
			System.exit(1);
		}
		return batcher;
	}
	
	public synchronized void reportWork(HashMap<String, RWSet> rwSetMap, int workSize) {
		for (String key : rwSetMap.keySet()) {
			RWSet set = rwSetMap.get(key);
			if ( set.writeSet.size() > 0 || set.readSet.size() > 0) {
				List<RWSet> setList = workMap.get(key);
				if (setList == null) {
					setList = new Vector<RWSet>();
				}
				setList.add(set);
				workMap.put(key, setList); 
			
				// update range;
				long minRow = Long.MAX_VALUE;
				long maxRow = 0;
				Pair<Long, Long> range = rangeMap.get(key);
				if (range == null) {
					range = new Pair<Long, Long>();
				} else {
					minRow = range.getFirst();
					maxRow = range.getSecond();
				}
				for (Long row : set.writeSet) {
					if(row < minRow)
						minRow = row;
					if(row > maxRow)
						maxRow = row;
				}
				for (Long row : set.readSet) {
					if(row < minRow)
						minRow = row;
					if(row > maxRow)
						maxRow = row;
				}
				range.setFirst(minRow);
				range.setSecond(maxRow);
				rangeMap.put(key, range);
			}
		}
		this.workSize += workSize;		
		// currentBatchDone = false;
		logger.logVerbose("<Batcher> Received work:"+rwSetMap);
	}	
	
	public synchronized boolean fetchResult(int tid) {
		while(!resultMap.containsKey(tid)) {
			logger.logVerbose("<Batcher> client waiting for result");
			try {	
				wait();
			}catch(Exception e){e.printStackTrace();}
			logger.logVerbose("<Batcher> results arrived");
		}
		return resultMap.remove(tid);
	}
	
	public synchronized void updateResult(String tableName, List<LockCallResult> resultList, boolean done) {
		// System.out.println("<Batcher> Got partial results for Table:"+tableName+" result:"+resultList);
		for(LockCallResult result : resultList) {								
			if (!result.allLocked) {	// if we get abort for this tid in this batch, record it
				logger.logVerbose("<Batcher> T"+result.tid+" will be aborted");
				decisionMap.put(result.tid, false);	// once a decision is set to false, it will be always false
				resultMap.put(result.tid, false);
				continue;
			} else {	// if we get commit for this tid in this batch 
				if (!decisionMap.containsKey(result.tid) || decisionMap.get(result.tid) == true ) {
					// this means so far we have not received any abort in any previous batch 
					decisionMap.put(result.tid, true);	
					resultMap.put(result.tid, true);	
				}
			}
			HashMap<Integer, List<Long>> tempMap = lockResultMap.get(tableName);	// map of tid->currently locked-items for this table
			if(tempMap == null)
				tempMap = new HashMap<Integer, List<Long>>();
			List<Long> items = tempMap.get(result.tid);	//currently locked items for this tid
			if (items == null)
				items = new Vector<Long>();
			items.addAll(result.lockedItems);	//add the newly locked items to the temp map for this tid	
			tempMap.put(result.tid, items);	
			lockResultMap.put(tableName, tempMap);
		}
		if (done) {		// no more calls for this table		
			logger.logVerbose("<Batcher> Done with Table:"+tableName+" for current batch");	
			doneCount++;
			if (doneCount == currentBatchCount) {	// no more calls for any table
				logger.logVerbose("<Batcher> Done with the current batch..");
				currentBatchDone = true;
				notifyAll();
			}
		}			
	}
	
	public synchronized void sendBatch() {		
		currentBatchCount = workMap.size();
		int currentWorkSize = workSize;
		doneCount=0;	
		currentBatchDone = false;
		logger.logVerbose("<Batcher> Sending batch of size:"+workSize+" batch count:"+currentBatchCount);
		
		// first clear the maps that will be updated by the callbacks
		lockedItemsMap.clear();
		lockResultMap.clear();
		decisionMap.clear();	//just in case that some transactions have not fetched the results		
		long startTime = System.currentTimeMillis();
		try {			
			for (final String key : workMap.keySet()) {
				final List<RWSet> rwSetList = workMap.get(key);
				final HTable table = tablePool.get(key);
				//Pair<Long, Long> range = rangeMap.get(key);
				//final byte[] start = Bytes.toBytes(range.getFirst());
				//final byte[] end = Bytes.toBytes(range.getSecond());
				//final int numRegions;
				//if (start == null && end == null) {
					//numRegions = table.getRegionLocations().size();
				//} else {
					//numRegions = table.getRegionsInRange(start, end).size();
				//}
				byte[][] regionKeys = regionKeysMap.get(key);
				final int numRegions = regionKeys.length;				 
				logger.logVerbose("<Batcher> sending to Table:"+key+" work:"+rwSetList +" numRegions:"+numRegions);
				
				final LockCallback cb = new LockCallback(key, numRegions, this);
				final Batcher parent = this;
				for(int i=0; i<regionKeys.length;i++) {
					final byte[] row = regionKeys[i];					
					executor.submit(new Runnable() {					
					@Override
					public void run() {
						try {
							LockingProtocol proto = table.coprocessorProxy(LockingProtocol.class, row);
							List<LockCallResult> resultList = proto.getSILocks(rwSetList);
							cb.update(null, row, resultList);
							//table.coprocessorExec(LockingProtocol.class, start, end, 
									//new LockCall(rwSetList), new LockCallback(key, numRegions, parent));
						} catch(Throwable t) {t.printStackTrace();}	
					}
					});
				}
				
				logger.logVerbose("<Batcher> sent batch for table "+key);
			}
			workMap.clear();			
			workSize = 0;
			HashMap<String, Pair<Long,Long>> tempRMap = rangeMap;
			rangeMap = new HashMap<String, Pair<Long,Long>>();
			while(!currentBatchDone) {
				logger.logVerbose("<Batcher> waiting for batch done..");
				wait();
			}
			logger.logVerbose("<Batcher> batch done..");
			long endTime = System.currentTimeMillis();
			StatsManager.reportBatchStats(currentWorkSize, endTime-startTime);
			for (String key : lockResultMap.keySet()) {
				HashMap<Integer, List<Long>> tempMap = lockResultMap.get(key);	//map tid->locked items for this table
				List<Long> items = new Vector<Long>();
				for (int tid : tempMap.keySet()) {
					if(decisionMap.get(tid) == false) {	//if this txn aborted, we need to release the acquired locks	
						List<Long> tempItems =  tempMap.get(tid);
						items.addAll(tempItems);
						logger.logVerbose("<Batcher> T"+tid+" is aborted, releasing "+tempItems.size()+" acquired locks");
					}
				}
				if (!items.isEmpty())
					lockedItemsMap.put(key, items);
			}
			
			if(!lockedItemsMap.isEmpty()) {
				logger.logVerbose("<Batcher> locks to be released, lockedItemsMap:"+lockedItemsMap);
				for (String key : lockedItemsMap.keySet()) {					
					final List<Long> lockedItems = lockedItemsMap.get(key);
					logger.logVerbose("<Batcher> releasing locks for table:"+key+" "+lockedItems);
					HTable table = tablePool.get(key);
					Pair<Long, Long> range = tempRMap.get(key);
					final byte[] start = Bytes.toBytes(range.getFirst());
					final byte[] end = Bytes.toBytes(range.getSecond());
					table.coprocessorExec(LockingProtocol.class, start, end,
							new Batch.Call<LockingProtocol, Void>() {
								public Void call(LockingProtocol instance) throws IOException {
									instance.releaseWriteLocks(lockedItems);
									return null;
								}
							});
				}
			}
			
			
		} catch(Exception e){
			e.printStackTrace();
			workMap.clear();
			currentBatchDone = true;
		} catch(Throwable t) {
			t.printStackTrace();
			workMap.clear();
			currentBatchDone = true;
		}		
	}
	
	public void run() {
		System.out.println("Batcher thread started..");
		while(!stop) {
			try {
				// System.out.println("Batcher thread sleeping..");
				Thread.sleep(BATCHING_DELAY);
				if (!workMap.isEmpty()) {
					sendBatch();
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
		}
		// System.exit(0);
	}
	
}

class LockCall implements Batch.Call<LockingProtocol, List<LockCallResult>> {
	public List<RWSet> rwSetList;
	
	public LockCall(List<RWSet> rwSets) {
		rwSetList = rwSets;
	}
	
	public List<LockCallResult> call(LockingProtocol instance) throws IOException {		
		// System.out.println("Call on instance");
		List<LockCallResult> res = instance.getSILocks(rwSetList);
		// System.out.println("Call on instance returned");
		return res;
	}
}

class LockCallback implements  Batch.Callback<List<LockCallResult>> {
	String tableName;
	Batcher parent;
	int numCallbacks;
	int count;
	public LockCallback(String table, int callbacks, Batcher parent) {
		tableName = table;
		numCallbacks = callbacks;
		this.parent = parent;
	}
	
	public synchronized void update(byte[] region, byte[] row, List<LockCallResult> resultList) {		
		count++;		
		if (count == numCallbacks) {		// we have received callbacks from all regions
			parent.updateResult(tableName, resultList, true);
		}	
		else {
			// System.out.println("expecting "+(numCallbacks-count)+" more..");
			parent.updateResult(tableName, resultList, false);
		}		
	}		
		
}
