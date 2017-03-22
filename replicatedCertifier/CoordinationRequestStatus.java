package replicatedCertifier;

import java.util.*;

public class CoordinationRequestStatus extends RequestStatus {
	public static final int NOT_INITIATED = 0;
	public static final int IN_PHASE_1 = 1;	
	public static final int PHASE_1_COMPLETE = 2;	// every participant has voted commit or abort
	public static final int IN_PHASE_2 = 3;
	public static final int COMPLETED = 4;
	
	HashMap<String, RequestStatus> reqStatusMap = new HashMap<String, RequestStatus>();
	int status;
	int validationOutcome;	// outcome of the phase1 of 2PC request 
	int cmtVoteCnt;		// num of participants who have voted 'Commit'
}
