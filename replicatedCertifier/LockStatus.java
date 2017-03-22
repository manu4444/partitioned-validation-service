package replicatedCertifier;

public class LockStatus {
	public static final int LOCK_ACQUIRED = 1;
	public static final int QUEUED = 2;
	public static final int INQUIRE = 3;
	public static final int RELIQUINSH = 4;
	
	String table;
	Long itemID;
	int status;
	public LockStatus(String table, Long itemID, int status) {
		super();
		this.table = table;
		this.itemID = itemID;
		this.status = status;
	}
	
	public boolean equals(Object o) {
		LockStatus l = (LockStatus)o;
		if (l.table.equals(table) && l.itemID == itemID) {
			return true;
		} else {
			return true;
		}
	}
	
}
