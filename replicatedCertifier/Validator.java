package replicatedCertifier;

import java.util.*;
import java.rmi.*;

import org.apache.hadoop.hbase.util.*;

import xact.Dependency;
import util.*;
//import xact.TransactionHandler;

public class Validator {
	//set { row-id => [ list of < version timestamps, list of <reader timestamps> > ]}
	//Hashtable<byte[], LinkedList< Pair< Long, LinkedList<Long>>>> rwTable;
	
	//Hashtable<String, Hashtable<byte[], LinkedList< Pair< Long, LinkedList<Long>>>>> rwMap;	
	
	// map <tableName => map <rowid, conflictinfo>>
	Hashtable<String, Hashtable<Long, ConflictRecord>> conflictTable;

	//Added By Manu
	RequestStatusTracker pendingReqTracker;
	ReplicationConfig config;
	public Validator(RequestStatusTracker pendTracker, ReplicationConfig config) {
		this.pendingReqTracker = pendTracker;
		this.config = config;
		conflictTable = new Hashtable<String, Hashtable<Long,ConflictRecord>>();
	}

	
	public Validator() {
		conflictTable = new Hashtable<String, Hashtable<Long,ConflictRecord>>();
	}	


	
	// try getting locks for a given table
	public int tryLock(RWSet rwSet, String table) {
		Hashtable<Long,ConflictRecord> singleTableMap = getConflictTableFor(table);		
		for( long w: rwSet.writeSet) {
			ConflictRecord rec = null;		
			synchronized(singleTableMap) {
				if(singleTableMap.containsKey(w)) {
					rec = singleTableMap.get(w);
				} else {
					rec = new ConflictRecord(0, 0, -1);					
					singleTableMap.put(w, rec);
				}				
			}
			synchronized (rec) {
				long wTS = rec.getWriteTS();
				long rTS = rec.getReadTS();
				if ( wTS > rwSet.startTS || rTS > rwSet.startTS) { //Manu Why second condition necessary?
					//concurrent committed transaction, i.e. transaction with commit ts > given start ts
					//has written to this item or read this item, so conflict
					abort(rwSet.tid);
					return 	ValidateResponse.ABORT;					
				} else {
					boolean locked = rec.tryWriteLock(rwSet.tid);
					if (!locked) {
						abort(rwSet.tid);
						return 	ValidateResponse.ABORT;
					} else {
						LockStatus ls = new LockStatus(table, w, LockStatus.LOCK_ACQUIRED);							
						pendingReqTracker.notifyLockStatus(rwSet.tid, ls);	
						//System.out.println("wlock acquired on "+w+" for T"+rwSet.tid);
					} 
				}
			}	
		} 
		
		
		for (long r: rwSet.readSet) {
			ConflictRecord rec = null;		
			synchronized(singleTableMap) {
				if(singleTableMap.containsKey(r)) {
					rec = singleTableMap.get(r);
				} else {
					rec = new ConflictRecord(0, 0, -1);
					singleTableMap.put(r, rec);
				}
			}		
			
			synchronized (rec) {
				long wTS = rec.getWriteTS();					
				if ( wTS > rwSet.startTS ) { 
					//concurrent committed transaction, i.e. transaction with commit ts > given start ts
					//has written to this item or read this item, so conflict
					abort(rwSet.tid);
					return 	ValidateResponse.ABORT;					
				} else {
					boolean locked = rec.tryReadLock(rwSet.tid);
					if (!locked) {
						abort(rwSet.tid);
						return ValidateResponse.ABORT;
					} else {
						LockStatus ls = new LockStatus(table, r, LockStatus.LOCK_ACQUIRED);							
						pendingReqTracker.notifyLockStatus(rwSet.tid, ls);	
						//System.out.println("wlock acquired on "+r+" for T"+rwSet.tid);
					}				
				}
			}
		} 		
		return ValidateResponse.ALL_LOCKS_ACQUIRED;		
	}
	
	public int tryLock(HashMap<String, RWSet> rwSetMap) {
		int response = 0;
		boolean conflict = false;		
		for(String table: rwSetMap.keySet()) {			
			RWSet rwSet = rwSetMap.get(table);
			response = tryLock(rwSet, table);
			if (response == ValidateResponse.ABORT) {
				break;
			}
		}
		return response;
	}			
	
	public ValidateResponse validate(HashMap<String, RWSet> rwSetMap) {
		int response = tryLock(rwSetMap);
		ValidateResponse retResponse = new ValidateResponse(response);
		if (retResponse.responseType != ValidateResponse.ABORT) {
			pendingReqTracker.setValidationStatus(rwSetMap.values().iterator().next().tid, retResponse.responseType);
		}
		return retResponse;
	}
	
	private void commitInternal(String table, RWSet rwSet, long commitTS) {
		Hashtable<Long, ConflictRecord> singleTableMap = getConflictTableFor(table);
		for (long w : rwSet.writeSet) {
			ConflictRecord cr = singleTableMap.get(w);
			if (cr == null) {
				cr = new ConflictRecord(0, 0, -1);
			}
			synchronized(cr) {
				cr.unlock(rwSet.tid);
				cr.setWriteTS(commitTS);
				singleTableMap.put(w, cr);				
			}
			ValidationEvent event = new ValidationEvent();
			event.eventType = ValidationEvent.LOCK_RELEASE_WITH_COMMIT;
			event.itemID = w;
			event.tableName = table;
			event.tid = rwSet.tid;
			eventProcessor.reportEvent(event);
		}
		for (long r : rwSet.readSet) {
			ConflictRecord cr = singleTableMap.get(r);
			if (cr == null) {
				cr = new ConflictRecord(0, 0, -1);
			}
			synchronized(cr) {
				cr.unlock(rwSet.tid);
				if (cr.readTS > commitTS) {
					cr.setReadTS(commitTS);
				}
				// System.out.println("rlock released with commit on "+r+" for T"+rwSet.tid);
				singleTableMap.put(r, cr);
			}
			ValidationEvent event = new ValidationEvent();
			event.eventType = ValidationEvent.LOCK_RELEASE_WITH_COMMIT;
			event.itemID = r;
			event.tableName = table;
			event.tid = rwSet.tid;
			eventProcessor.reportEvent(event);
		}
	}

	
	// Convenience method for getting handle to a conflict table for |tableName|
	private Hashtable<Long,ConflictRecord> getConflictTableFor(String tableName) {
		Hashtable<Long,ConflictRecord> singleTableMap = null;
		if(!conflictTable.containsKey(tableName)){
			 singleTableMap = new Hashtable<Long,ConflictRecord>();
			 conflictTable.put(tableName, singleTableMap);
		}			
		singleTableMap = conflictTable.get(tableName);
		
		return singleTableMap;
	}
	
	public static void main(String args) {
		
	}
}
