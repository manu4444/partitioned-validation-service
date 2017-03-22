package replicatedCertifier;

public class ValidationEvent {

	public static final int LOCK_RELEASED = 1;
	public static final int LOCK_ACQUIRED = 2;
	public static final int SEND_RETRY = 3;
	public static final int RECV_RETRY = 4;
	public static final int RECV_ABORT = 5;
	public static final int RECV_LOCKS_ACQUIRED = 6;
	public static final int SEND_INQUIRE = 7;
	public static final int RECV_RELINQUISH = 8;
	public static final int LOCK_RELEASE_WITH_COMMIT = 9;
	
	public int eventType;
	public long itemID;
	public int tid;
	public String tableName;

	public ValidationEvent(int type, long item) {
		eventType = type;
		itemID = item;
	}
	
	public ValidationEvent() {
		
	}
}
