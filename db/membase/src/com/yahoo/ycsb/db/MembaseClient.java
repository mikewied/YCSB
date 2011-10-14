/**
 * Membase client binding for YCSB.
 *
 * Author: Michael Wiederhold
 */

package com.yahoo.ycsb.db;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

import net.spy.memcached.MemcachedClient;

import com.yahoo.ycsb.DB;

public class MembaseClient extends DB {
	private static final int OK = 0;
	private static final int ERROR = -1;

	private static final String MEMBASE_ADDRESS = "memcached.address";
	private static final String MEMBASE_ADDRESS_DEFAULT = "localhost";

	private static final String MEMBASE_BUCKET = "membase.bucket";
	private static final String MEMBASE_BUCKET_DEFAULT = "default";

	private static final String MEMBASE_PASSWORD = "membase.password";
	private static final String MEMBASE_PASSWORD_DEFAULT = "";

	private MemcachedClient client;

	public void init() {
		String address = getProperties().getProperty(MEMBASE_ADDRESS, MEMBASE_ADDRESS_DEFAULT);
		String bucketName = getProperties().getProperty(MEMBASE_BUCKET, MEMBASE_BUCKET_DEFAULT);
		String password = getProperties().getProperty(MEMBASE_PASSWORD, MEMBASE_PASSWORD_DEFAULT);

		try {
			List<URI> uris = new LinkedList<URI>();
			uris.add(new URI("http://" + address + ":8091/pools"));
			client = new MemcachedClient(uris, bucketName, password);
		} catch (UnknownHostException e) {
			System.err.println("IP address of host could not be determined");
		} catch (IOException e1) {
			System.err.println("Could not connect to host at " + address);
		} catch (URISyntaxException e) {
			System.err.println("Bad URI: http://" + address + ":8091/pools");
		}
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, String> result) {
		String value = null;
		try {
			value = (String)client.asyncGet(key).get();
			if (value == null) {
				System.err.println(key + " not found while doing GET");
				return ERROR;
			}
			result = getResult(value);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			return ERROR;
		}
		
		return OK;
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, String>> result) {
		throw new UnsupportedOperationException("Scan in not supported in Membase. Try Couchbase!");
	}

	@Override
	public int update(String table, String key, HashMap<String, String> values) {
		String value = createValue(values);
		try  {
			if (!client.set(key, 0, value).get()) {
				System.err.println("UPDATE failed");
				return ERROR;
			}
		} catch (Exception e) {
			return ERROR;
		}
		return OK;
	}

	@Override
	public int insert(String table, String key, HashMap<String, String> values) {
		String value = createValue(values);
		try  {
			if (!client.set(key, 0, value).get()) {
				System.err.println("SET failed");
				return ERROR;
			}
		} catch (Exception e) {
			return ERROR;
		}
		return OK;
	}

	@Override
	public int delete(String table, String key) {
		try {
			if (!client.delete(key).get()) {
				System.err.println("DELETE failed");
				return ERROR;
			}
		} catch (Exception e) {
			return ERROR;
		}
		return OK;
	}

	private String createValue(HashMap<String, String> kvs) {
		StringBuilder value = new StringBuilder();
		boolean first = true;

		for (Entry<String, String> kv : kvs.entrySet()) {
			if (!first) {
				value.append("\t");
			}
			first = false;
			value.append(kv.getKey());
			value.append("\n");
			value.append(kv.getValue());
		}
		return value.toString();
	}

	private HashMap<String, String> getResult(String value) {
		HashMap<String, String> result = new HashMap<String, String>();
		String[] values = value.split("\t");
		for (int i = 0; i < values.length; i++) {
			String[] kv = values[i].split("\n");
			result.put(kv[0], kv[1]);
		}
		return result;
	}
}
