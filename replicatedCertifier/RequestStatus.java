package replicatedCertifier;

import java.util.*;
import util.*;

public class RequestStatus {
	protected int tid;
	String endpointServerName;
	protected int validationStatus;
	private HashMap<Long, LockStatus> lockStatusList = new HashMap<Long, LockStatus>();
	private HashMap<String, RWSet> rwSetMap;
	protected int lockedCount = 0;
	protected int size; 
	
	public HashMap<String, RWSet> getRWSetMap() {
		return rwSetMap;
	}
	
	public void setRWSetMap(HashMap<String, RWSet> rwSetMap) {
		size = 0;
		this.rwSetMap = rwSetMap;
		for (RWSet rwSet : rwSetMap.values()) {
			size += rwSet.writeSet.size();
			size += rwSet.readSet.size();
		}
	}
	public LockStatus getLockStatus(long item) {
		return lockStatusList.get(item);
	}
	
	public synchronized Collection<LockStatus> getAllLockStatus() {
		return lockStatusList.values();
	}
	
	public synchronized void updateLockStatus(long item, LockStatus ls) {
		LockStatus oldLS = getLockStatus(item);
		lockStatusList.put(item, ls);
		if (ls.status == LockStatus.LOCK_ACQUIRED && (oldLS == null || oldLS.status != LockStatus.LOCK_ACQUIRED)) {
			// acquired a previously non-acquired lock
			lockedCount++;
		} else if (oldLS != null && oldLS.status == LockStatus.LOCK_ACQUIRED && ls.status == LockStatus.QUEUED) {
			// relinquished and queued a previously acquired lock 
			lockedCount--;
		}
	}
	
	public synchronized void clear() {
		lockStatusList.clear();
	}
	
	public String toString() {
		return "RequestStatus#T"+tid+"[es="+endpointServerName+" status="+validationStatus;
	}
}
