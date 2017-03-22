package replicatedCertifier;

import java.util.*;

public class RequestStatusTracker {
	Hashtable<Integer, RequestStatus> pendingReqTable;
	
	public RequestStatusTracker() {
		pendingReqTable = new Hashtable<Integer, RequestStatus>();
	}
	
	public void reportNewRequest(int tid, String endpoint) {
		RequestStatus rs = new RequestStatus();
		rs.tid = tid;
		rs.endpointServerName = endpoint;
		pendingReqTable.put(tid, rs);
	}
	
	public void reportNewRequest(RequestStatus rs) {
		if (!pendingReqTable.containsKey(rs.tid))
			pendingReqTable.put(rs.tid, rs);
	}
	
	public void notifyLockStatus(int tid, LockStatus ls) {
		RequestStatus rs = pendingReqTable.get(tid);		
		rs.updateLockStatus(ls.itemID, ls);
		
		pendingReqTable.put(tid, rs);
	}
	
	public void delete(int tid) {
		pendingReqTable.remove(tid);
	}
	
	public void clear(int tid) {
		RequestStatus rs = pendingReqTable.get(tid);
		rs.clear();
	}
	
	public void setValidationStatus(int tid, int status) {
		RequestStatus rs = pendingReqTable.get(tid);
		rs.validationStatus = status;
		pendingReqTable.put(tid, rs);
	}
	
	public RequestStatus getRequestStatus(int tid) {
		return pendingReqTable.get(tid);
	}
	
}
