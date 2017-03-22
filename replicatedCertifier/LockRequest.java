package replicatedCertifier;

public class LockRequest implements Comparable{
	boolean wmode;	//true for write false for read
	int tid;
	
	@Override
	public int compareTo(Object o) {
		LockRequest l = (LockRequest)o;
		if (tid < l.tid)
			return -1;
		else if (tid == l.tid)
			return 0;
		else 
			return 1;
	}
	
	public boolean equals(Object o) {
		LockRequest l = (LockRequest)o;
		return l.tid == tid;
	}
}
