package replicatedCertifier;

import java.util.*;
import java.io.*;

public class ValidateResponse implements Serializable {
	public static final int ALL_LOCKS_ACQUIRED = 1;	//all locks have been acquired
	public static final int QUEUED = 2;	//at least one lock req is queued
	public static final int ABORT = 4;	//abort
	public static final int NULL_RESPONSE = 0;	//no immediate response to give, response will come later through call back
	public static final int RELINQUISH = 5;	//relinquish some locks
	
	public int responseType;
	public List<LockStatus> lockStatus = new Vector<LockStatus>();
	
	
	public ValidateResponse(int response) {
		this.responseType = response;
	}

	public ValidateResponse(int response, List<Integer> lockedItems) {
		this.responseType = response;		
	}
}
