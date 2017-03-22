package xact;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.hbase.client.*;

public interface DSGManagerInterface {
	
	public static final String START_TS_COLNAME = "start-ts"; //only in active table
	public static final String CMT_TS_COLNAME = "cmt-ts"; //
	public static final String OUT_EDGE_COLNAME = "o-edge";	
	public static final String WSET_COLNAME = "w-set";	
	public static final String STATUS_COLNAME = "status";
	
	
	public boolean addXactNode(int tid, long startTS) throws IOException;
	public void changeXactTS(int tid,long commitTS,int status) throws IOException;
	public void addWriteSet(int tid, byte[] writeSet) throws IOException;
	public boolean changeXactStatus(int tid, int newStatus, int oldStatus) throws IOException; 
			
	public boolean checkCycle(int startTid) throws IOException;
	public void addDependency(Vector<Dependency> depList) throws IOException;
	public void doCommit(int tid, long ts) throws IOException;
	public void doAbort(int tid, long ts) throws IOException;
	public void deleteXactNode(int tid) throws IOException;
	public void logCmtStatus(long ts, int status) throws IOException;
		
	//query methods
	public int getXactStatus(int tid, Long changeTime) throws IOException;
	//public int getXactTID(long ts) throws IOException;
	public byte[] getWriteSet(int tid) throws IOException;
	public void pruneDSG() throws IOException;
	
		
}
