package replicatedCertifier;

import java.rmi.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


import util.*;

public class EventProcessor {
	BlockingQueue<ValidationEvent> eventQueue;
	Validator validator;
	ReplicationConfig config;
	
	public EventProcessor(int numThreads, Validator validator, ReplicationConfig config) {
		eventQueue = new LinkedBlockingQueue<ValidationEvent>();
		this.validator = validator;
		this.config = config;
		for (int i=0; i<numThreads; i++) {
			EventProcessorThread thr = new EventProcessorThread(this);
			thr.start();
		}
	} 
	
	public void reportEvent(ValidationEvent event) {
		// System.out.println("Event reported:"+event.eventType);
		eventQueue.offer(event);
	}
}

class EventProcessorThread extends Thread {
	BlockingQueue<ValidationEvent> eventQueue;
	Validator validator;
	EventProcessor parent;
	
	public EventProcessorThread(EventProcessor parent) {
		this.parent = parent;
		eventQueue = parent.eventQueue;
		validator = parent.validator;
	}	

	
	private void processLockRelease(ValidationEvent event) throws Exception {
		long item = event.itemID;
		ConflictRecord rec = parent.validator.conflictTable.get(event.tableName).get(item);
		synchronized (rec) {
			if (rec.waitingTidQueue.size() > 0) {
				LockRequest lr = rec.getWaitingReq();
				//TODO rec.lock(lr.tid, lr.wmode);
				// System.out.println("lock acquired on "+item+" by T"+lr.tid+" with wmode:"+lr.wmode);
				LockStatus ls = new LockStatus(event.tableName, item, LockStatus.LOCK_ACQUIRED);
				validator.pendingReqTracker.notifyLockStatus(lr.tid, ls);
				processLockAcquired(item, lr.tid, event.tableName);
			}
		}
	}
	
	private void processLockReleaseWithCommit(ValidationEvent event) throws Exception {
		long item = event.itemID;
		ConflictRecord rec = parent.validator.conflictTable.get(event.tableName).get(item);
		List<LockRequest> lrToBeRemoved = new Vector<LockRequest>();
		synchronized(rec) {
			if (rec.writer != -1) {
				return;	// we are processing a stale lock release event
			}
			if (rec.isWriteLocked()) {
				// lock was in write mode, all pending lock req should be aborted
				lrToBeRemoved.addAll(rec.waitingTidQueue);
			}
			else {	// lock released in read mode
				for (LockRequest lr : rec.waitingTidQueue) {
					if (lr.wmode) 
						lrToBeRemoved.add(lr);
				}
			}
			for(LockRequest lr : lrToBeRemoved) {
				// remove the corresponding lock req from queue
				rec.removeWaitingReq(lr.tid);
				RequestStatus rs = validator.pendingReqTracker.getRequestStatus(lr.tid);
				if (rs != null) {
					rs.validationStatus = ValidateResponse.ABORT;
				}
			}
			// if we have any lock req pending give it lock
			// Note if it was a writer commit then there shouldn't be any pending lock req
			while (rec.waitingTidQueue.size()>0) {
				LockRequest lr = rec.getWaitingReq();
				RequestStatus rs = validator.pendingReqTracker.getRequestStatus(lr.tid);				
				if (rs.validationStatus == ValidateResponse.ABORT) {
						continue;
				}				
				if (lr.wmode) {
					rec.tryWriteLock(lr.tid);
				} else {
					rec.tryReadLock(lr.tid);
				}
				// System.out.println("lock acquired on "+item+" by T"+lr.tid+" with wmode:"+lr.wmode);
				LockStatus ls = new LockStatus(event.tableName, item, LockStatus.LOCK_ACQUIRED);
				validator.pendingReqTracker.notifyLockStatus(lr.tid, ls);
				processLockAcquired(item, lr.tid, event.tableName);
				break;
			}
		}	
		
		// send abort messages for aborted reqs
		for(LockRequest lr : lrToBeRemoved) {
			int tid = lr.tid;
			RequestStatus rs = validator.pendingReqTracker.getRequestStatus(tid);
			if (rs == null) {
				continue;
			}
			synchronized(rs) {
				validator.abort(tid);
			}
			String server = rs.endpointServerName;					
			ValidateResponse response = new ValidateResponse(ValidateResponse.ABORT);
			PeerInterface handle = parent.config.getHandle(server);
			handle.callback(tid, parent.config.myHostName, response);
			// System.out.println("Sent abort msg for T"+tid);
		}		
	}
	
	private void processLockAcquired(ValidationEvent event) {
		processLockAcquired(event.itemID, event.tid, event.tableName);
	}
	
	private void processLockAcquired(long item, int tid, String table) {		
		RequestStatus rst = validator.pendingReqTracker.getRequestStatus(tid);
		if (rst ==null) {
			// we are processing a stale event
			return;
		}
		if (rst.lockedCount == rst.size) {
			PeerInterface peer = parent.config.getHandle(rst.endpointServerName);
			ValidateResponse response = new ValidateResponse(ValidateResponse.ALL_LOCKS_ACQUIRED);
			try {
				peer.callback(tid, parent.config.myHostName, response);
			} catch(RemoteException e) {} 
		}
		ConflictRecord rec = parent.validator.conflictTable.get(table).get(item);		
		synchronized (rec) {
			Vector<Integer> tidsToQueue = new Vector<Integer>();
			for (LockRequest lr : rec.waitingTidQueue) {
				if (lr.tid > tid) {
					tidsToQueue.add(lr.tid);
				}
			}
			for (int t : tidsToQueue) {
				RequestStatus rs = validator.pendingReqTracker.getRequestStatus(t); 
				String server = rs.endpointServerName;				
				LockStatus ls = rs.getLockStatus(item);
				if (ls.status != LockStatus.QUEUED) {					
					// status changed
					PeerInterface peer = parent.config.getHandle(server);
					ValidateResponse response = new ValidateResponse(ValidateResponse.QUEUED);
					ls.status = LockStatus.QUEUED;
					response.lockStatus.add(ls);		
					validator.pendingReqTracker.notifyLockStatus(tid, ls);					
					try {
						peer.callback(tid, parent.config.myHostName, response);
					} catch(RemoteException e) {}
				}
			}
		}
	}
	
	
	
	public void processEvent(ValidationEvent event) throws Exception {
		
		switch (event.eventType) {
		case ValidationEvent.LOCK_RELEASED:
			processLockRelease(event);
			break;
			
		case ValidationEvent.LOCK_ACQUIRED:
			processLockAcquired(event);
			break;
			
		case ValidationEvent.LOCK_RELEASE_WITH_COMMIT:
			processLockReleaseWithCommit(event);
			break;
			
		case ValidationEvent.SEND_INQUIRE:
			System.out.println("Asynch inq: should not have happened");
			//TODO validator.inquire(event.tid, event.tableName, event.itemID);
			break;
			
		}
	}
	
	/*
	private void processRevalidate(int tid) throws RemoteException {
		//System.out.println("Revalidating T"+tid);
		RequestStatus rs = validator.pendingReqTracker.getRequestStatus(tid); 
		if ( rs == null) {
			//request already finished
			System.out.println("T"+tid+" already finished");
			return;
		}
		
		HashMap<String, RWSet> rwSetMap = rs.rwSetMap;
		String server = rs.endpointServerName;
		ValidateResponse response = parent.validator.validate(rwSetMap);
		if (response.responseType != ValidateResponse.ON_HOLD) {
			
			PeerInterface handle = parent.config.getHandle(server);
			handle.callback(tid, parent.config.myHostName, response);
		}
	}
	
	
	private void processSendRetry(ValidationEvent event) throws RemoteException {
		int tid = event.tid;
		parent.validator.retry(tid);	//release all locks held at this server
		String server = parent.validator.pendingReqTracker.getRequestStatus(tid).endpointServerName;
		PeerInterface handle = parent.config.getHandle(server);
		ValidateResponse response = new ValidateResponse(ValidateResponse.RETRY);
		handle.callback(tid, parent.config.myHostName, response);
	}	
	*/
	
	

	public void run() {
		while(true) {
			try {
				ValidationEvent event = eventQueue.take();
				processEvent(event);	
			} catch(Exception e) {
				e.printStackTrace();
	
			}  
		}
	}
}
