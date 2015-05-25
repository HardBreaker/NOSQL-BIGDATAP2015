package nosql.big_column.hbase;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import nosql.key_value.redis.City;
import nosql.utils.HBASE_Citytable_meta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.gson.Gson;
import com.google.protobuf.ServiceException;

public class HbaseClient {
	private static final String ZOOKEEPER_QUORUM = "192.168.33.30";
	private static final String CLIENT_PORT = "2181";
	private static final int TIMEOUT = 12000;

	private Configuration config;
	private HBaseAdmin hbase_admin;

	public HbaseClient() {
		Configuration config = createConfiguration();

		try {
			HBaseAdmin.checkHBaseAvailable(config);
			this.config = config;
			hbase_admin = new HBaseAdmin(config);
		} catch (ServiceException | IOException e) {

			System.out
					.println("Something went wrong, cannot connect to Server");
			System.out.println("ERROR: " + e.getMessage());
			System.exit(1);
		}

	}

	private Configuration createConfiguration() {
		Configuration config = HBaseConfiguration.create();
		config.clear();
		config.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
		config.set("hbase.zookeeper.property.clientPort", CLIENT_PORT);
		config.setInt("timeout", TIMEOUT);
		return config;
	}

	private List<City> getDataFromFile() {
		Gson gson = new Gson();
		List<City> cities = new ArrayList<>();

		try (Scanner sc = new Scanner(new File("plz.data"))) {
			while (sc.hasNextLine()) {

				String jsonObj = (String) sc.nextLine();
				System.out.println(jsonObj);
				City city = gson.fromJson(jsonObj, City.class);
				cities.add(city);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return cities;
	}

	public List<Put> prepareData() {
		List<Put> puts = new ArrayList<Put>();
		List<City> cities = getDataFromFile();

		for (Iterator iterator = cities.iterator(); iterator.hasNext();) {
			City city = (City) iterator.next();
			Put p = new Put(Bytes.toBytes(city.get_Id()));
			p.add(Bytes.toBytes(HBASE_Citytable_meta.DESC_COLUMN_FAMILY),
					Bytes.toBytes(HBASE_Citytable_meta.DESC_COLUMN_NAME),
					Bytes.toBytes(city.getCity()));
			p.add(Bytes.toBytes(HBASE_Citytable_meta.DESC_COLUMN_FAMILY),
					Bytes.toBytes(HBASE_Citytable_meta.DESC_COLUMN_LOC),
					Bytes.toBytes(city.getLoc().toString()));
			p.add(Bytes.toBytes(HBASE_Citytable_meta.DESC_COLUMN_FAMILY),
					Bytes.toBytes(HBASE_Citytable_meta.DESC_COLUMN_POP),
					Bytes.toBytes(city.getPop()));
			p.add(Bytes.toBytes(HBASE_Citytable_meta.DESC_COLUMN_FAMILY),
					Bytes.toBytes(HBASE_Citytable_meta.DESC_COLUMN_STATE),
					Bytes.toBytes(city.getState()));

			if (city.getCity().equals("HAMBURG")
					|| city.getCity().equals("BREMEN")) {
				p.add(toBytes(HBASE_Citytable_meta.FUSSBALL_DESC_COLUMN_FAMILY),
						toBytes(HBASE_Citytable_meta.FUSSBALL_COLUMN_AVAILABLE),
						toBytes("ja"));
			}
			puts.add(p);

		}
		return puts;
	}

	public void insertToHBaseTable(String tableName, List<Put> data) {
		// List<Put> data = prepareDataBase();
		HTable htable = null;
		try {
			htable = new HTable(config, Bytes.toBytes(tableName));
		} catch (IOException e) {
			System.out.println("Something went wrong by creating table "
					+ tableName);
			e.printStackTrace();
		}
		try {
			htable.put(data);
		} catch (RetriesExhaustedWithDetailsException | InterruptedIOException e) {
			System.out
					.println("Something went wrong by inserting data into table "
							+ tableName);
			e.printStackTrace();
		}
		if (htable != null) {
			try {
				htable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public boolean createCityTable(String tableName) {
		HTableDescriptor[] tables;

		try {
			tables = hbase_admin.listTables();
			for (int i = 0; i < tables.length; i++) {
				if (tables[i].getTableName().equals(tableName)) {
					return true;
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Instantiating table descriptor class
		HTableDescriptor tableDescriptor = new HTableDescriptor(
				TableName.valueOf(HBASE_Citytable_meta.TABLE_NAME));

		// Adding column families to table descriptor
		tableDescriptor.addFamily(new HColumnDescriptor(
				HBASE_Citytable_meta.DESC_COLUMN_FAMILY));
		tableDescriptor.addFamily(new HColumnDescriptor(
				HBASE_Citytable_meta.FUSSBALL_DESC_COLUMN_FAMILY));

		// Execute the table through admin
		try {
			hbase_admin.createTable(tableDescriptor);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	private byte[] toBytes(String value) {
		return Bytes.toBytes(value);
	}

	public void closeHbaseAdmin() {
		if (hbase_admin != null) {
			try {
				hbase_admin.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private HTable getHTableObject(String tableName) {
		HTable table = null;
		try {
			table = new HTable(config, tableName);
		} catch (IOException e) {

			e.printStackTrace();
			// return null;
		}
		return table;
	}

	public City getCityByPLZ(int plznb) {

		// Instantiating HTable class
		HTable table = getHTableObject(HBASE_Citytable_meta.TABLE_NAME);

		// Instantiating Get class
		Get g = new Get(Bytes.toBytes(plznb));

		// Reading the data
		Result result = null;
		try {
			result = table.get(g);

			byte[] value = result.getValue(
					Bytes.toBytes(HBASE_Citytable_meta.DESC_COLUMN_FAMILY),
					Bytes.toBytes(HBASE_Citytable_meta.DESC_COLUMN_NAME));

			byte[] value1 = result.getValue(
					Bytes.toBytes(HBASE_Citytable_meta.DESC_COLUMN_FAMILY),
					Bytes.toBytes(HBASE_Citytable_meta.DESC_COLUMN_STATE));

			// Printing the values
			String name = Bytes.toString(value);
			String state = Bytes.toString(value1);
			System.out.println("IN DATABASE ");
			City city = new City();
			city.set_Id(plznb);
			city.setCity(name);
			city.setState(state);
			return city;
		} catch (IOException e) {
			// System.out.println("No City was found for " + plznb);
			return null;
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public List<String> getListFromCityname(String upperCaseCityName) {
		List<String> foundCities = new ArrayList<String>();
		HTable table = getHTableObject(HBASE_Citytable_meta.TABLE_NAME);

		Scan scan = new Scan();

		// Scanning the required columns
		scan.addColumn(toBytes(HBASE_Citytable_meta.DESC_COLUMN_FAMILY),
				toBytes(HBASE_Citytable_meta.DESC_COLUMN_NAME));

		// Getting the scan result
		ResultScanner scanner = null;
		try {
			scanner = table.getScanner(scan);
			// Reading values from scan result
			for (Result result = scanner.next(); result != null; result = scanner
					.next()) {
				String cityName = Bytes.toString(result.getValue(
						toBytes(HBASE_Citytable_meta.DESC_COLUMN_FAMILY),
						toBytes(HBASE_Citytable_meta.DESC_COLUMN_NAME)));
				System.out.println("City Found: " + Bytes.toInt(result.getRow()));
				if (upperCaseCityName.equals(cityName)) {
					foundCities.add(String.valueOf(Bytes.toInt(result.getRow())));

				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			scanner.close();
			try {
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// closing the scanner
		return foundCities;
	}

	public void fillDatabase() {
		boolean exists = createCityTable(HBASE_Citytable_meta.TABLE_NAME);
		if (!exists) {
			List<Put> rows = prepareData();
			insertToHBaseTable(HBASE_Citytable_meta.TABLE_NAME, rows);
		}
		closeHbaseAdmin();
	}
}
