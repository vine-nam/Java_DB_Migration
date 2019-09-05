package main;

import java.sql.SQLException;
import java.util.List;

public class Main {

	final private static String pattern = ",[ |\t]*(?i)where";

	public static void main(String[] args) {

		String migrationFile = args[0];
		String dbConnectionFile = args[1];

		if (args.length >= 3) {
			DBExecute.batchSize = Integer.parseInt(args[2]);
		}

		DBExecute dbe = null;
		ReadFile rf = new ReadFile();
		List<String> lines;
		String tempSourceDBName = "";
		String tempTargetDBName = "";

		try {
			dbe = new DBExecute(dbConnectionFile);
			lines = rf.readFileExceptCommend(migrationFile);
			for (String line : lines) {
				Runtime.getRuntime().gc();
				String item[] = rf.splitLine(line);
				if (item.length >= 5) {
					Com.printLog(item[1] + " → " + item[3] + " (" + item[4] + ")");
					if (isNotSame(tempSourceDBName, item[0])) {
						try {
							dbe.closeSourceConnection();
							dbe.setSourceConnection(item[0]);
							tempSourceDBName = item[0];
						} catch (Exception e) {
							tempSourceDBName = "";
							Com.printErrorLog(e.getMessage());
							continue;
						}
					}
					if (isNotSame(tempTargetDBName, item[2])) {
						try {
							dbe.closeTargetConnection();
							dbe.setTargetConnection(item[2]);
							tempTargetDBName = item[2];
						} catch (Exception e) {
							tempTargetDBName = "";
							Com.printErrorLog(e.getMessage());
							continue;
						}
					}
					try {
						dbe.setConditionalStatment(getConditionalStatment(item, line));
						dbe.executeMigration(item[0], item[1], item[3]);
					} catch (Exception e) {
						Com.printErrorLog(e.getMessage());
					}
				}
				System.out.println();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				dbe.closeSourceConnection();
				dbe.closeTargetConnection();
			} catch (SQLException e) {
			}
		}
	}

	private static boolean isNotSame(String dbName, String inputDBName) {
		if (dbName.isEmpty() || !dbName.equals(inputDBName)) {
			return true;
		}
		return false;
	}

	private static String getConditionalStatment(String[] item, String line) {
		String[] conditions = line.split(pattern);
		if (conditions.length > 1) {
			return conditions[1];
		}
		return null;
	}

}
