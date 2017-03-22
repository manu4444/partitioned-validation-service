package replicatedCertifier;

import java.util.*;
import java.util.concurrent.*;
import java.net.*;
import java.io.*;

import util.*;

public class Coordinator {
	RequestStatusTracker pendingReqTracker;
	ReplicationConfig config;
	ExecutorService executors;
	
	int tid;
	long commitTS = -1;
	boolean commit = false;
	HashMap<String, RWSet> rwSetMap;
	HashMap<String, HashMap<String, RWSet>> partitionedRWSetMap;
	int phase=0;
	CoordinationRequestStatus crqStatus;
	static long counter = 0;
	public Coordinator(RequestStatusTracker pendTracker, ReplicationConfig config, ExecutorService executors, int tid, HashMap<String, RWSet> rwSetMap) {
		pendingReqTracker = pendTracker;
		this.tid = tid;
		this.rwSetMap = rwSetMap;
		this.config = config;
		this.executors = executors;
		crqStatus = new CoordinationRequestStatus();
		crqStatus.tid = tid;
	}
	
	public void validate() throws Exception {
		// prepare request		
		partitionedRWSetMap = new HashMap<String, HashMap<String,RWSet>>();
		
		for(String tableName : rwSetMap.keySet()) { //Manu Here, there will be only one table hence this forloop will be not needed
			RWSet rwSet = rwSetMap.get(tableName);
			for (long w: rwSet.writeSet) {
				String server = config.getServerForItem(w);
				//System.out.println("Server "+server+" for item "+w);
				HashMap<String, RWSet> pRWSetMap = partitionedRWSetMap.get(server);
				if (pRWSetMap == null) {
					pRWSetMap = new HashMap<String, RWSet>();
				}
				RWSet pRWSet = pRWSetMap.get(tableName);
				if (pRWSet == null) {
					pRWSet = new RWSet();
					pRWSet.tid = rwSet.tid;
					pRWSet.startTS = rwSet.startTS;
				}
				pRWSet.writeSet.add(w);
				pRWSetMap.put(tableName, pRWSet);
				partitionedRWSetMap.put(server, pRWSetMap);
			}
			for (long r: rwSet.readSet) {
				String server = config.getServerForItem(r);
				//System.out.println("Server "+server+" for item "+r);
				HashMap<String, RWSet> pRWSetMap = partitionedRWSetMap.get(server);
				if (pRWSetMap == null) {
					pRWSetMap = new HashMap<String, RWSet>();
				}
				RWSet pRWSet = pRWSetMap.get(tableName);
				if (pRWSet == null) {
					pRWSet = new RWSet();
					pRWSet.tid = rwSet.tid;
					pRWSet.startTS = rwSet.startTS;		
				}
				pRWSet.readSet.add(r);
				pRWSetMap.put(tableName, pRWSet);
				partitionedRWSetMap.put(server, pRWSetMap);
			}
		}						
		pendingReqTracker.reportNewRequest(crqStatus);		
		prepare();				
		if(crqStatus.validationOutcome == ValidateResponse.ABORT) {
			// System.out.println("In abort for T"+tid);
			abort();
			commit = false;
		} else {
			// System.out.println("In commit for T"+tid);
			commit();
			commit = true;
		}
		// System.out.println("Waiting after commit/abort for T"+tid);
		// waitForStatus(CoordinationRequestStatus.COMPLETED);
		pendingReqTracker.delete(tid);
		// System.out.println("Completed phase2 response for T"+tid);
	}
	
	private void prepare() throws InterruptedException {
		phase = 1;		
		crqStatus.status = CoordinationRequestStatus.IN_PHASE_1;
		crqStatus.cmtVoteCnt = 0;		
		for (String server : partitionedRWSetMap.keySet()) {
			final String serverName = server;
			//System.out.println("Looking handle for "+server);
			final PeerInterface handle = config.getHandle(server);
			final HashMap<String, RWSet> rwSetMap = partitionedRWSetMap.get(server);
			final String myServerName = config.myHostName;
			RequestStatus rs = new RequestStatus();
			rs.endpointServerName = server;
			rs.tid = tid;			
			rs.setRWSetMap(rwSetMap);
			crqStatus.reqStatusMap.put(server, rs);
			executors.submit(new Runnable() {
				public void run() {
					try {
						ValidateResponse response = handle.validate(rwSetMap, myServerName);
						notifyValidationResponse(serverName, response);
					} catch(Exception e){
						e.printStackTrace();						
					}
				}
			});
		}
		// System.out.println("Waiting in prepare for T"+tid);
		waitForStatus(CoordinationRequestStatus.PHASE_1_COMPLETE);		
	}
	
	private void commit() throws IOException {
		phase = 2;
		obtainCommitTS();		
		for (String server : partitionedRWSetMap.keySet()) {
			final String serverName = server;
			final PeerInterface handle = config.getHandle(server);
			executors.submit(new Runnable() {
				public void run() {
					try {						
						handle.commit(tid, commitTS);
						// System.out.println("Received cmt ack from"+serverName);
						notifyPhase2Response(serverName);
					} catch(Exception e){
						e.printStackTrace();						
					}
				}
			});
		}
	}
	
	private void abort() {
		phase = 2;
		for (String server : partitionedRWSetMap.keySet()) {
			final String serverName = server;
			final PeerInterface handle = config.getHandle(server);
			executors.submit(new Runnable() {
				public void run() {
					try {
						handle.abort(tid);
						// System.out.println("Received abort ack from"+serverName);
						notifyPhase2Response(serverName);
					} catch(Exception e){
						e.printStackTrace();						
					}
				}
			});
		}
	}
	
	private void retry() throws InterruptedException {
		for (String server : partitionedRWSetMap.keySet()) {			
			final PeerInterface handle = config.getHandle(server);
			executors.submit(new Runnable() {
				public void run() {
					try {
						handle.retry(tid);						
					} catch(Exception e){
						e.printStackTrace();						
					}
				}
			});
		}		
		Thread.sleep(20);	
		prepare();
	}
	
	public void waitForStatus(int status) throws InterruptedException {
		synchronized (crqStatus) {
			while (crqStatus.status != status) {				
				crqStatus.wait(60000);
				if (crqStatus.status != status) {
					// System.out.println("T"+tid+" timedout in waiting for status "+status+" crqstatus:"+crqStatus.status);
					// System.out.println(crqStatus.reqStatusMap);
					System.exit(0);
					break;					
				}
			}
		}		
	}
	
	public static void processValidationResponse(CoordinationRequestStatus crqStatus, String server, ValidateResponse response) {
		// proceed to phase 2 either if you get all votes/locks or if you get any abort
		synchronized (crqStatus) {
			if (crqStatus.status != CoordinationRequestStatus.IN_PHASE_1) {
				// we are getting a delayed response, we have already moved to phase2: Abort
				// Note: commit can't happen without getting response from all
				return;
			}			
			RequestStatus rs = crqStatus.reqStatusMap.get(server);			
			rs.validationStatus = response.responseType;
			crqStatus.reqStatusMap.put(server,  rs);
			if (response.responseType == ValidateResponse.ABORT) {
				// System.out.println("Received abort for T"+crqStatus.tid);
				crqStatus.validationOutcome = ValidateResponse.ABORT;
				crqStatus.status = CoordinationRequestStatus.PHASE_1_COMPLETE;
				crqStatus.notify();				
			} else if (response.responseType == ValidateResponse.QUEUED) {
				crqStatus.validationOutcome = ValidateResponse.QUEUED;
				//crqStatus.status = CoordinationRequestStatus.PHASE_1_COMPLETE;
				//crqStatus.notify();		
			} else if (response.responseType == ValidateResponse.ALL_LOCKS_ACQUIRED) {
				crqStatus.cmtVoteCnt++;
				if (crqStatus.cmtVoteCnt == crqStatus.reqStatusMap.size()) {
					crqStatus.validationOutcome = ValidateResponse.ALL_LOCKS_ACQUIRED;
					crqStatus.status = CoordinationRequestStatus.PHASE_1_COMPLETE;
					crqStatus.notify();
				}
			}
		}
	}
	
	public void notifyValidationResponse(String server, ValidateResponse response) {
		processValidationResponse(crqStatus, server, response);
	}
	
	public void notifyPhase2Response(String server) {
		// System.out.println("Received phase2 response from server:"+server+" for T"+tid);
		synchronized (crqStatus) {			
			RequestStatus rs = crqStatus.reqStatusMap.remove(server);			
			if (crqStatus.reqStatusMap.isEmpty()) {
				crqStatus.status = CoordinationRequestStatus.COMPLETED;
				crqStatus.notify();
			
			}
		}
	}
	
	private void obtainCommitTS() throws IOException {
		/* TODO uncomment
		String host = Config.getInstance().getStringValue("timestampServerName");
		int port = Config.getInstance().getIntValue("timestampServerPort");
		Socket socket = new Socket(host, port);
		DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
		dos.writeInt(3);
        DataInputStream dis = new DataInputStream(socket.getInputStream());
        long ts = dis.readLong();
        dis.close();
        dos.close();
        socket.close();        
		commitTS = ts;
		*/
		counter++;
		commitTS = counter;
	}
	
	
}
