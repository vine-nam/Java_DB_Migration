package main;

import java.sql.SQLException;
import java.util.List;

public class Main {

	public static void main(String[] args) {

		String migrationFile = args[0];
		String dbConnectionFile = args[1];
		
		if(args.length >= 3) {
			DBExecute.batchSize = Integer.parseInt(args[2]); 
		}
		
		DBExecute dbe = null;
		ReadFile rf = new ReadFile();
		List<String> lines;
		String dbNameS = "";
		String dbNameT = "";
		
		try {
			dbe = new DBExecute(dbConnectionFile);
			lines = rf.readFileExceptCommend(migrationFile);
			for (String line : lines) {
				Runtime.getRuntime().gc();
				String item[] = rf.splitLine(line);
				if (item.length >= 4) {
					if(dbNameS.isEmpty() || !dbNameS.equals(item[0])) {
						dbNameS = item[0];
						dbe.closeConnection(dbe.conS);
						dbe.setSourceConnection(dbNameS);
						if(dbe.conS == null) {
							Com.printLog("Connection 실패: " + dbNameS);
							dbNameS = null;
							continue;
						}
					}
					if(dbNameT.isEmpty() || !dbNameT.equals(item[2])) {
						dbNameT = item[2];
						dbe.closeConnection(dbe.conT);
						dbe.setTargetConnection(dbNameT);
						if(dbe.conT == null) {
							Com.printLog("Connection 실패: " + dbNameT);
							dbNameT = null;
							continue;
						}
					}
					Com.printLog(item[1] + " -> " + item[3]);
					dbe.executeMigration(item[1], item[3]);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dbe.closeConnection(dbe.conS);
				dbe.closeConnection(dbe.conT);
			} catch (SQLException e) {
			}
		}
	}

}
