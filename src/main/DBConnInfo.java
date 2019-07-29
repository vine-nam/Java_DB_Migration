package main;
import java.util.HashMap;
import java.util.List;

public class DBConnInfo {
	
	private HashMap<String, HashMap<String, String>> dbMap = new HashMap<String, HashMap<String,String>>();
	private HashMap<String, String> infoMap = null;

	public HashMap<String, String> getDbInfo(String dbName) {
		return dbMap.get(dbName);
	}

	public void toMap(List<String> lines) {
		for (String line : lines) {
			String[] str = line.split("/");
			infoMap = new HashMap<String, String>();
			infoMap.put("user", str[1]);
			infoMap.put("pass", str[2]);
			infoMap.put("url", str[3]);
			dbMap.put(str[0], infoMap);	
		}
	}

	
}
