package replicatedCertifier;

import java.io.Serializable;
import java.util.*;

public class ConflictRecord implements Serializable {
	long writeTS;
	long readTS;
	boolean locked;
	int writer;
	HashSet<Integer> readers;
	boolean wmode;	// mode: true if for write mode false for readmode;
	
	TreeSet<LockRequest> waitingTidQueue;
		
	public ConflictRecord(long writeTS, long readTS, int writer) {
		super();
		this.writeTS = writeTS;
		this.readTS = readTS;		
		this.writer = writer;
		readers = new HashSet<Integer>();
		waitingTidQueue = new TreeSet<LockRequest>();
	}
	
	public synchronized long getWriteTS() {
		return writeTS;
	}
	
	public synchronized void setWriteTS(long writeTS) {
		this.writeTS = writeTS;
	}
	
	public synchronized long getReadTS() {
		return readTS;
	}
	
	public synchronized void setReadTS(long readTS) {
		this.readTS = readTS;
	}
	
	public synchronized boolean isLocked() {
		return locked;
	} 
	
	
	public synchronized boolean isReadLocked() {
		return locked && !wmode;
	}
	
	
	public synchronized boolean isWriteLocked() {
		return locked && wmode;
	}
	
	
	private synchronized void readLock(int reader) {
		locked = true;
		wmode = false;
		readers.add(reader);
	}
	
	public synchronized boolean tryReadLock(int reader) {
		if (isWriteLocked() && writer != reader) {
			return false;
		} else {
			readLock(reader);
			return true;
		}
	}
	
	private synchronized void writeLock(int writer) {
		locked = true;
		wmode = true;
		this.writer = writer;
	}
	
	public synchronized boolean tryWriteLock(int writer) {
		// if not locked or locked by me in write mode or locked by me in read mode and I am the only reader
		// then get write lock
		if (!isLocked() || this.writer == writer || (readers.size()==1 && readers.contains(writer))) {
			writeLock(writer);
			return true;			
		} else {
			// locked by someone else in read mode or write mode
			return false;
		}
	}
	
	public synchronized void unlock(int tid) {		
		locked = false;
		if (wmode) {
			this.writer = -1;	
		}
		readers.remove(tid);
		// IMP: don't reset the mode
	}
	
	public synchronized void addLockReq(int tid, boolean wmode) {
		LockRequest lr = new LockRequest();
		lr.tid = tid;
		lr.wmode = wmode;
		waitingTidQueue.add(lr);
	}

	public LockRequest getWaitingReq() {
		return waitingTidQueue.pollFirst();		
	}
	
	public void removeWaitingReq(int tid) {
		LockRequest dummy = new LockRequest();
		dummy.tid = tid;
		waitingTidQueue.remove(dummy);
	}
	
	
}
