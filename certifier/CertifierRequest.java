package certifier;

import java.io.*;
import java.util.*;

import org.apache.hadoop.hbase.util.*;

public class CertifierRequest implements Serializable{
	
	public static final int CERTIFY_SI = 1;
	public static final int CERTIFY_SERIALIZABLE = 2;
	public static final int CERTIFY_BOTH = 3;
	
	
	int actionType;
	Hashtable<String, Pair<Collection<byte[]>,Collection<byte[]>>> rwSet;
	long startTS;
	long commitTS;
	int tid;
	
	
	public CertifierRequest(int actionType, Hashtable<String, Pair<Collection<byte[]>,Collection<byte[]>>> rwSet,
			 long startTS, long commitTS, int tid){
		this.actionType = actionType;
		this.rwSet = rwSet;
		this.startTS = startTS;
		this.commitTS = commitTS;
		this.tid = tid;
	}

	public int getActionType() {
		return actionType;
	}

	public void setActionType(int actionType) {
		this.actionType = actionType;
	}
	
	public Hashtable<String, Pair<Collection<byte[]>, Collection<byte[]>>> getRwSet() {
		return rwSet;
	}

	public long getCommitTS() {
		return commitTS;
	}
	
	public long getStartTS() {
		return startTS;
	}	
	
	public int getTid() {
		return tid;
	}
	
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("Request for transaction#");
		buffer.append(commitTS);
		buffer.append(" rwset=");
		buffer.append(rwSet);		
		return buffer.toString();
	}	
}
