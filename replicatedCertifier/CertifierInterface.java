package replicatedCertifier;

import java.rmi.*;
import java.util.*;

import util.*;

public interface CertifierInterface extends Remote {
	public long certify(int tid, HashMap<String, RWSet> rwSetMap) throws RemoteException;
}
