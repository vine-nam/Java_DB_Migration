package main;

import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.stream.Collectors;

public class DBExecute {

	public static int batchSize = 50000;

	Connection conS = null;
	Connection conT = null;
	ResultSet rs = null;
	Statement stm = null;
	PreparedStatement pstmt = null;
	
	DBConnInfo dbConnInfo = new DBConnInfo();
	HashMap<String, String> dbConnData = null;
	HashMap<String, String> talbeColumns = null;
	
	String oracleUrl = "jdbc:oracle:thin:@";
	
	Blob blob = null;

	public DBExecute(String filePath) throws IOException {
		dbConnInfo.toMap(new ReadFile().readFileExceptCommend(filePath));
	}

	public Connection getConnection(String dbName) {
		Connection con = null;
		dbConnData = dbConnInfo.getDbInfo(dbName);
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(oracleUrl + dbConnData.get("url"), dbConnData.get("user"),
					dbConnData.get("pass"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return con;
	}

	public void setSourceConnection(String dbName) {
		conS = getConnection(dbName);
	}
	
	public void setTargetConnection(String dbName) {
		conT = getConnection(dbName);
	}

	public void closeConnection(Connection con) throws SQLException {
		if (con != null) {
			con.close();
			con = null;
		}
	}

	public void executeMigration(String sourceTable, String targetTable) {
		try {
			getColumns(conS, sourceTable);
			truncateTable(conT, getTableName(conT, targetTable));
			insertIntoTable(conT, selectTable(conS, sourceTable), targetTable);
		} catch (Exception e) {
			Com.printDate();
			e.printStackTrace();
		}
	}

	public ResultSet excuteSql(Connection con, String sql) throws SQLException {
		stm = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
		return stm.executeQuery(sql);
	}

	public void getColumns(Connection con, String table) throws SQLException {
		talbeColumns = new HashMap<String, String>();
		String[] item = table.toUpperCase().split("\\.");

		String sqlColumns = "SELECT COLUMN_NAME, DATA_TYPE FROM all_tab_columns WHERE TABLE_NAME = '" + item[1]
				+ "' AND OWNER = '" + item[0] + "'";

		rs = excuteSql(con, sqlColumns);
		while (rs.next()) {
			talbeColumns.put(rs.getString(1), rs.getString(2));
		}
		rs.close();
		rs = null;
	}

	public ResultSet selectTable(Connection con, String table) throws SQLException {
		String sql = "SELECT * FROM " + table;
		return excuteSql(con, sql);
	}

	public String createInsertSql(String table) {
		String colList = talbeColumns.entrySet().stream().map(x -> x.getKey()).collect(Collectors.joining(","));
		String paramList = talbeColumns.entrySet().stream().map(x -> "?").collect(Collectors.joining(","));
		String sql = "INSERT INTO " + table + "(" + colList + ") VALUES (" + paramList + ")";
		return sql;
	}

	public void insertIntoTable(Connection con, ResultSet rs, String table) {
		int i = 0;
		int j = 0;

		try {
			pstmt = con.prepareStatement(createInsertSql(table));
			con.setAutoCommit(false);

			Com.printDate();
			while (rs.next()) {
				i++;
				j = 0;
				for (String key : talbeColumns.keySet()) {
					if (talbeColumns.get(key).contains("TIMESTAMP")) {
						pstmt.setTimestamp(++j, rs.getTimestamp(key));
					} else if (talbeColumns.get(key).contains("DATE")) {
						pstmt.setTimestamp(++j, rs.getTimestamp(key));
					} else if (talbeColumns.get(key).contains("BLOB")) {
						if (blob == null) {
							blob = con.createBlob();
						}
						blob.setBytes(1, rs.getBytes(key));
						pstmt.setBlob(++j, blob);
					} else {
						pstmt.setString(++j, rs.getString(key));
					}
				}

				pstmt.addBatch();
				pstmt.clearParameters();

				if ((i % batchSize) == 0) {
					System.out.print("★");
					pstmt.executeBatch();
					pstmt.clearBatch();
				}
			}
			System.out.println("★");
			Com.printLog("총 갯수:\t" + i);

			pstmt.executeBatch();
			con.commit();
		} catch (SQLException e) {
			System.out.println();
			e.printStackTrace();
			try {
				con.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		} finally {
			if (rs != null)
				try {
					rs.close();
					rs = null;
				} catch (SQLException ex) {
				}
			if (pstmt != null)
				try {
					pstmt.close();
					pstmt = null;
				} catch (SQLException ex) {
				}
			System.out.println();
		}
	}

	public void truncateTable(Connection con, String table) throws SQLException {
		String sqlTruncateTarget = "TRUNCATE TABLE " + table;
		excuteSql(con, sqlTruncateTarget);
	}

	public String getTableName(Connection con, String table) throws SQLException {
		table = table.toUpperCase().split("\\.")[1];
		String sql = "SELECT TABLE_NAME FROM USER_SYNONYMS WHERE SYNONYM_NAME = '" + table + "'";
		
		rs = excuteSql(con, sql);
		if (rs.next()) {
			table = rs.getString("TABLE_NAME");
		}
		rs.close();
		rs = null;
		return table;
	}
}
