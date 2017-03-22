package replicatedCertifier;

import java.rmi.RemoteException;
import java.util.*;

import util.RWSet;

public class ValidatorTest {
	
	class DummyPI implements PeerInterface {
		
		@Override
		public ValidateResponse validate(HashMap<String, RWSet> rwSet,
				String serverName) {			
			return null;
		}

		@Override
		public void retry(int tid) {}

		@Override
		public void commit(int tid, long commitTS) {}

		@Override
		public void abort(int tid) {}

		@Override
		public void callback(int tid, String serverName,
				ValidateResponse response) {
			System.out.println("Got validate response: T"+tid+"=>"+response.responseType);			
		}

		@Override
		public boolean inquire(int tid, String table, long itemID)
				throws RemoteException {
			System.out.println("Got inquire for: T"+tid);
			return true;
		}
		
	}
	
	Validator validator;
	RequestStatusTracker reqTracker;
	
	public ValidatorTest() {
		ReplicationConfig config = new ReplicationConfig();		
		DummyPI dummyPI = new DummyPI();
		config.addHandle("TestServer", dummyPI);
		reqTracker = new RequestStatusTracker();
		validator = new Validator(reqTracker, config);
	}
	
	public void testDeadlock() {
		/* T1 wset=[1,2]
		 * T2 wset=[2,3]
		 * T3 wset=[3,1]
		 */
		List<Long> wset1 = new Vector<Long>();
		wset1.add(new Long(1));
		List<Long> wset2 = new Vector<Long>();
		wset2.add(new Long(2));
		List<Long> wset3 = new Vector<Long>();
		wset3.add(new Long(3));
		RWSet rwSet1 = new RWSet(0, 1, wset1);
		RWSet rwSet2 = new RWSet(0, 2, wset2);
		RWSet rwSet3 = new RWSet(0, 3, wset3);
		
		ValidateResponse response;
		
		// lock 1 by T1
		response = makeLockReq(1, rwSet1);
		if (response.responseType != ValidateResponse.ALL_LOCKS_ACQUIRED) {
			System.out.println("FAIL: couldnot acquire lock on 1 by T1");
			System.exit(0);
		}
		System.out.println("TRACE: locked 1 by T1");
		// lock 2 by T2
		response = makeLockReq(2, rwSet2);
		if (response.responseType != ValidateResponse.ALL_LOCKS_ACQUIRED) {
			System.out.println("FAIL: couldnot acquire lock on 2 by T1");
			System.exit(0);
		}
		System.out.println("TRACE: locked 2 by T2");
		// lock 3 by T3
		response = makeLockReq(3, rwSet3);
		if (response.responseType != ValidateResponse.ALL_LOCKS_ACQUIRED) {
			System.out.println("FAIL: couldnot acquire lock on 3 by T1");
			System.exit(0);
		}
		System.out.println("TRACE: locked 3 by T3");
		wset1.clear();
		wset1.add(new Long(2));
		wset2.clear();
		wset2.add(new Long(3));
		wset3.clear();
		wset3.add(new Long(1));
		// lock 2 by T1 -> inq message will be sent to T2, T1 will get the lock 		
		response = makeLockReq(1, rwSet1);
		if (response.responseType != ValidateResponse.ALL_LOCKS_ACQUIRED) {
			System.out.println("FAIL: T1 on hold for 2");
			System.exit(0);
		}
		System.out.println("TRACE: lock 2 by T1 -> T1 got lock from T2");
		// lock 3 by T2 -> put on hold		
		response = makeLockReq(2, rwSet2);
		if (response.responseType != ValidateResponse.ALL_LOCKS_ACQUIRED) {
			System.out.println("FAIL: T2 on hold for 3");
			System.exit(0);
		}
		System.out.println("TRACE: lock 3 by T2 -> T2 got lock from T3");
		// lock 1 by T3 -> retry
		response = makeLockReq(3, rwSet3);
		if (response.responseType != ValidateResponse.QUEUED) {
			System.out.println("FAIL: T3 did not get queued response");
			System.exit(0);
		}		
		System.out.println("TRACE: lock 1 by T3 -> queued");
		//validator.retry(3);
		
		try { Thread.sleep(50); } catch(Exception e){}
		System.out.println("Commiting T1");
		validator.commit(1, 1);
		
		try { Thread.sleep(50); } catch(Exception e){}
		validator.abort(2);
		try { Thread.sleep(50); } catch(Exception e){}
		/*
		response = makeLockReq(3, rwSet3);
		if (response.responseType != ValidateResponse.ABORT) {
			System.out.println("FAIL: T3 did not get abort response");
			System.exit(0);
		}
		System.out.println("TEST SUCCESSFUL");
		*/
	}
	
	 
	private ValidateResponse makeLockReq(int tid, RWSet rwSet){
		HashMap<String, RWSet> rwSetMap = new HashMap<String, RWSet>();
		rwSetMap.put("TestTable", rwSet);
		RequestStatus rs = new RequestStatus();
		rs.setRWSetMap(rwSetMap);
		rs.tid = tid;
		rs.endpointServerName = "TestServer";
		reqTracker.reportNewRequest(rs);
		return validator.validate(rwSetMap);
	}
	
	public static void main(String args[]) {
		ValidatorTest test = new ValidatorTest();
		test.testDeadlock();
	}
}
