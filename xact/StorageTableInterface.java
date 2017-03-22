package xact;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.hbase.client.*;

public interface StorageTableInterface {
		
	//public void setTableName(String table);	
	
	public void setSnapshotTS(long ts);
	public void setTID(int tid);
	/** Reads the version corresponding to the snapshot given by snapshotTS
	 * @param get - Get object specifying the get operation
	 * @param snapshotTS - timestamp indicating which snapshot to read from 
	 * @return Result object for the corresponding version
	 */
	public Result get(Get get) throws IOException;
	
	/** Eager: Writes immediately to HBase 
	 * Lazy:Buffers the write set until commit time
	 * @param put - Put object specifying data to put
	 */
	public void put(Put put) throws IOException;
	public void put(List<Put> puts) throws IOException;
	
	
	public void setCommitTS(long ts);
	
	/** lock writes	   
	 */
	public boolean lockWrites() throws IOException;
	
	/** lock reads	   
	 */
	public boolean lockReads() throws IOException;
	
	/** writes the buffered write-set with the commit-pending status with commitTS as the version timestamp
	 * @param commitTS - specifies the version timestamp
	 */
	public void flushWrites() throws IOException; //flush write-set with commit_pending status
	
	/** Commits the writes written i.e. changes the status of flushed writes to committed status	 * 
	 */
	public void commitWrites() throws IOException;

	/** Commits the acquired read locks* 
	 */
	public void commitReadLocks() throws IOException;
	
	/** gets ww conflict (if any) with concurrent transaction 
	 * @param commitTS commit timestamp of the transaction to be compared with
	 * @return return -1 if no conflict, else tid of the transaction which conflicts
	 */
	public int getWWConflict() throws IOException; 
	
	/** Get the dependency (edges) for the transaction identified by tid 
	 * @param tid - id of the transaction
	 * @return return -1 if no conflict, else tid of the transaction which conflicts
	 */
	public Vector<Dependency> getDependency() throws IOException;
	
	/** Rollbacks the writes
	 * @param commitTS - commit timestamp
	 * @throws IOException
	 */
	public void rollbackWrites() throws IOException;
	
	public void deleteVersion(byte[] row, long ts) throws IOException;
	
	public void commitVersion(byte[] row, long ts) throws IOException;
	
	public Vector<Put> getWriteSet() ;
	public Vector<Get> getReadSet() ;
	public String getTableName();
	
	public void close();
	
}
