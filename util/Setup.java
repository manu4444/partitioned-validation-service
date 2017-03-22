package util;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.conf.*;


public class Setup {
	public static void reset() throws Exception {
		// You need a configuration object to tell the client where to connect.
		// When you create a HBaseConfiguration, it reads in whatever you've set
		// into your hbase-site.xml and in hbase-default.xml, as long as these can
		// be found on the CLASSPATH

		Configuration config = HBaseConfiguration.create();

		// This instantiates an HTable object that connects you to
		// the "myLittleHBaseTable" table.
		HTable table = new HTable(config, "COUNTER");
		//RowLock lock = table.lockRow(Bytes.toBytes("row2"));
		//System.out.println("locked row: row1 lock=>"+lock);
		//table.unlockRow(lock);
		//System.out.println("unlocked row: row1 lock=>"+lock);
		Put p = new Put(Bytes.toBytes("cts"));
		long l = 0;
		p.add(Bytes.toBytes("d"), Bytes.toBytes("val"), Bytes.toBytes(l));

		table.put(p);

		p = new Put(Bytes.toBytes("sts"));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("val"), Bytes.toBytes(l));

		table.put(p);

		p = new Put(Bytes.toBytes("ts"));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("val"), Bytes.toBytes(l));

		table.put(p);

		p = new Put(Bytes.toBytes("tid"));
		p.add(Bytes.toBytes("d"), Bytes.toBytes("val"), Bytes.toBytes(l));

		table.put(p);
		System.out.println();
		HTable table2 = new HTable(config, "table1");
		Delete d = new Delete(Bytes.toBytes("row1"));
		table2.delete(d);
		System.out.println();

		HTable table3 = new HTable(config, "DSG");
		ResultScanner rs = table3.getScanner(Bytes.toBytes("d"));
		Result r = null;
		while ((r = rs.next()) != null) {
			table3.delete(new Delete(r.getRow()));
		}

		HTable table4 = new HTable(config, "CMTLOG");
		ResultScanner rs2 = table4.getScanner(Bytes.toBytes("d"));
		Result r2 = null;
		while ((r2 = rs2.next()) != null) {
			table4.delete(new Delete(r2.getRow()));
		}

		//System.out.println("Value Returned:"+new String(r.value()));
	}
	
	public static void CreateTables() throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		HColumnDescriptor colFam_D = new HColumnDescriptor("d");
		colFam_D.setMaxVersions(20);
		HTableDescriptor tableDSG = new HTableDescriptor("DSG");
		tableDSG.addFamily(colFam_D);
		admin.createTable(tableDSG);
		System.out.println("DSG table created");
		HTableDescriptor tableCMT = new HTableDescriptor("CMTLOG");
		HColumnDescriptor colFam2 = new HColumnDescriptor("d");
		colFam2.setMaxVersions(1);
		tableCMT.addFamily(colFam2);
		admin.createTable(tableCMT);		
		System.out.println("CMTLOG table created");
		HTableDescriptor tableCOUNT = new HTableDescriptor("COUNTER");
		tableCOUNT.addFamily(colFam2);
		admin.createTable(tableCOUNT);
		System.out.println("COUNTER table created");
		HColumnDescriptor colFam_MD = new HColumnDescriptor("md");
		colFam_D.setMaxVersions(20);
		HTableDescriptor table1 = new HTableDescriptor("table1");
		table1.addFamily(colFam_D);
		table1.addFamily(colFam_MD);
		admin.createTable(table1);
		System.out.println("storage table  table1 created");
	}
}


