package nosql.big_column.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

public class TestHBase {
	public static void main(String[] args) {
		try {
			Configuration config = new Configuration();
			config.clear();
			config.set("hbase.zookeeper.quorum", "192.168.33.30");
			config.set("hbase.zookeeper.property.clientPort", "2181");
			config.setInt("timeout", 12000);
//			config.set("zookeeper.znode.parent", "/hbase-unsecure");
//			config.set("hbase.master", "192.168.33.30:60000");
			// HBaseConfiguration config = HBaseConfiguration.create();
			// config.set("hbase.zookeeper.quorum", "localhost"); // Here we are
			// running zookeeper locally
			HBaseAdmin.checkHBaseAvailable(config);

			System.out.println("HBase is running!");
			// createTable(config);
			// creating a new table
			HTable table = new HTable(config, "mytable");
			System.out.println("Table mytable obtained ");

		} catch (MasterNotRunningException e) {
			System.out.println("HBase is not running!");
			e.printStackTrace();
//			System.exit(1);
		} catch (Exception ce) {
			ce.printStackTrace();
		}

	}
}
