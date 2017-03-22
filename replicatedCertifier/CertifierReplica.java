package replicatedCertifier;

import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;
import util.*;

public class CertifierReplica extends UnicastRemoteObject implements PeerInterface, CertifierInterface {
	
	Validator validator;
	ExecutorService executorPool;
	ReplicationConfig replicaConfig;
	RequestStatusTracker validationReqTracker, coordinationReqTracker;
	
	public CertifierReplica(String configFile) throws Exception {		
		Config config = new Config(configFile);		
		String serverListFile = config.getStringValue("serverListFile");
		Naming.bind("CertifierInterface", this); //Manu called by the client
		Naming.bind("PeerInterface", this);
		replicaConfig = new ReplicationConfig(serverListFile, this);
		System.out.println(replicaConfig.getServerForItem(0));
		System.out.println(replicaConfig.getServerForItem(1));
		System.out.println(replicaConfig.getServerForItem(2));
		System.out.println(replicaConfig.getServerForItem(3));
		validationReqTracker = new RequestStatusTracker();
		coordinationReqTracker = new RequestStatusTracker();
		executorPool = Executors.newCachedThreadPool();
		validator = new Validator(validationReqTracker, replicaConfig);
	}
	
	@Override
	public long certify(int tid, HashMap<String, RWSet> rwSetMap) {
		// System.out.println("Received new request");
		Coordinator coordinator = new Coordinator(coordinationReqTracker, replicaConfig, executorPool, tid, rwSetMap);
		try {
			coordinator.validate();
			if (coordinator.commit)
				return coordinator.commitTS;
			else
				return 0;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}	
	
	@Override
	public ValidateResponse validate(HashMap<String, RWSet> rwSet, String serverName) {
		RequestStatus rs = new RequestStatus();
		rs.tid = rwSet.values().iterator().next().tid;
		rs.setRWSetMap(rwSet);
		rs.endpointServerName = serverName;	 
		validationReqTracker.reportNewRequest(rs);
		synchronized (rs) {
			return validator.validate(rwSet);
		}
	}

	@Override
	public void retry(int tid) {		
		if(validationReqTracker.getRequestStatus(tid)!= null) {
			validator.retry(tid);
		}
	}

	@Override
	public void commit(int tid, long commitTS) {		
		validator.commit(tid, commitTS);
	}

	@Override
	public void abort(int tid) {
		if (validationReqTracker.getRequestStatus(tid) != null) { 
			validator.abort(tid);
		}
	}

	@Override
	public void callback(int tid, String server, ValidateResponse response) {		
		CoordinationRequestStatus crqStatus = (CoordinationRequestStatus) coordinationReqTracker.getRequestStatus(tid);
		if (crqStatus != null) {
			Coordinator.processValidationResponse(crqStatus, server, response);
		}
	}
	
	public static void main(String[] args) throws Exception {
		String configFile = args[0];
		CertifierReplica replica = new CertifierReplica(configFile);		
	}

	@Override
	public boolean inquire(int tid, String table, long itemID) throws RemoteException {
		CoordinationRequestStatus crqStatus = (CoordinationRequestStatus) coordinationReqTracker.getRequestStatus(tid);
		if (crqStatus != null) {
			if (crqStatus.validationOutcome != ValidateResponse.ALL_LOCKS_ACQUIRED) {
				crqStatus.validationOutcome = ValidateResponse.QUEUED;
				return true;
			} else 
				return false;
				
		} else 
			return true;
	}

}
