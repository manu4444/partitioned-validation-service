package util;

public class Constants {

	//constants
	//metadata column names in storage table
	/*  Table structure :
	 *   ---------------------------------------------------------------------
	 *  | row    |    default column family     |  metadata column family    |
	 *  ----------------------------------------------------------------------
	 *  | rowid  | tid-> <data related columns> | ts-> <tid,readset> | lock  | 
	 *   ---------------------------------------------------------------------
	 */	 
	public static final String READSET_COLNAME = "rs";
	public static final String READTS_COLNAME = "readts";
	public static final String LOCK_COLNAME = "lock";
	public static final String RLOCK_COLNAME = "rlock";
	public static final String TID_COLNAME = "tid";
	public static final String METADATA_FAMNAME = "md";

}
