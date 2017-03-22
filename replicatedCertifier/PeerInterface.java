package replicatedCertifier;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.*;
import util.*;

public interface PeerInterface extends Remote {
	// calls received from coordinator
	ValidateResponse validate(HashMap<String, RWSet> rwSet, String serverName) throws RemoteException;
	void retry(int tid) throws RemoteException;
	void commit(int tid, long commitTS) throws RemoteException;
	void abort(int tid) throws RemoteException;
	boolean inquire(int tid, String table, long itemID) throws RemoteException;
	
	// callbacks
	void callback(int tid, String serverName, ValidateResponse response) throws RemoteException;
	
}
