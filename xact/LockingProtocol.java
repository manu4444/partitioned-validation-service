package xact;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import util.*;

import java.util.List;
import java.io.IOException;

public interface LockingProtocol extends CoprocessorProtocol {
	public List<LockCallResult> getSILocks(List<RWSet> rwSet) throws IOException;
	public List<LockCallResult> getRWLocks(List<RWSet> rwSet) throws IOException;
	public void releaseWriteLocks(List<Long> rows) throws IOException;
}
