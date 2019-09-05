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
	public HashMap<String, String> talbeColumns = null;
	
	String oracleUrl = "jdbc:oracle:thin:@";
	
	Blob blob = null;
	
	String conditions = null; 

	public DBExecute(String filePath) throws IOException {
		dbConnInfo.toMap(new ReadFile().readFileExceptCommend(filePath));
	}

	public Connection getConnection(String dbName) throws Exception {
		Connection con = null;
		dbConnData = dbConnInfo.getDbInfo(dbName);
		if(dbConnData == null) {
			throw new Exception("'" + dbName + "'은(는) DB접속정보 파일에 없는 이름입니다 ");
		}
		
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			con = DriverManager.getConnection(oracleUrl + dbConnData.get("url"), dbConnData.get("user"),
					dbConnData.get("pass"));
		} catch (Exception e) {
			throw new Exception("'" + dbName + "' DB 접속 실패: " + e.getMessage());
		}
		return con;
	}

	public void setSourceConnection(String dbName) throws Exception {
		conS = getConnection(dbName);
		conS.setAutoCommit(false);
	}
	
	public void setTargetConnection(String dbName) throws Exception {
		conT = getConnection(dbName);
		conT.setAutoCommit(false);
	}
	
	public void setConditionalStatment(String conditions) {
		this.conditions = conditions;
	}

	public void closeSourceConnection() throws SQLException {
		if (conS != null) {
			conS.close();
			conS = null;
		}
	}
	
	public void closeTargetConnection() throws SQLException {
		if (conT != null) {
			conT.close();
			conT = null;
		}
	}

	public void executeMigration(String sourceDbName, String sourceTable, String targetTable) throws Exception {
		getColumns(conS, sourceTable);
		truncateTable(conT, getTableName(conT, targetTable.toUpperCase()));
		try {
			truncateTable(conS, changeIRtoIS(sourceDbName, targetTable.toUpperCase()));
		} catch (Exception e) {
			Com.printWarnLog(e.getMessage());
		}
		insertIntoTable(conT, selectTable(conS, sourceTable), targetTable);
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
		if(conditions != null && !conditions.isEmpty()) {
			sql += " WHERE " + conditions;
		}
		return excuteSql(con, sql);
	}

	public String createInsertSql(String table) {
		String colList = talbeColumns.entrySet().stream().map(x -> x.getKey()).collect(Collectors.joining(","));
		String paramList = talbeColumns.entrySet().stream().map(x -> "?").collect(Collectors.joining(","));
		String sql = "INSERT INTO " + table + "(" + colList + ") VALUES (" + paramList + ")";
		return sql;
	}

	private String changeIRtoIS(String sourceDbName, String table) {
		String[] str = table.split("\\.");
		String strTable = table;
		String ifStr;
		String user;
		if(str.length > 1) {
			strTable = str[1];
		} 
		ifStr = strTable.split("_")[0];
		if(ifStr != null) {
			user = dbConnInfo.getDbInfo(sourceDbName).get("user");
			if(ifStr.equals("IR")) {
				return user + "." + strTable.replaceFirst("IR", "IS");
			}
			if(ifStr.equals("IV")) {
				return user + "." + strTable.replaceFirst("IV", "IS");
			}
		}
		return table;
	}

	public void insertIntoTable(Connection con, ResultSet rs, String table) throws Exception {
		int i = 0;
		int j = 0;

		try {
			pstmt = con.prepareStatement(createInsertSql(table));

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
			
			pstmt.executeBatch();
			conT.commit();
			
			Com.printLog("총 개수:\t" + i);
		} catch (SQLException e) {
			try {
				con.rollback();
			} catch (SQLException e1) {
				throw new Exception(e1);
			}
			throw new Exception("'" + table + "'테이블 INSERT 실패: " + e.getMessage());
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
		}
	}

	public void truncateTable(Connection con, String table) throws Exception {
		String sql = "TRUNCATE TABLE " + table;
		try {
			excuteSql(con, sql);
		} catch (SQLException e) {
			throw new Exception("'" + table + "'테이블 TRUNCATE 실패: " + e.getMessage());
		}
	}

	public String getTableName(Connection con, String table) throws SQLException {
		String[] item = table.split("\\.");
		String userName = null;
		String tableName = null;
		
		if(item.length > 1) {
			userName = item[0];
			tableName = item[1];
			String sql = "SELECT TABLE_NAME FROM USER_SYNONYMS WHERE SYNONYM_NAME = '" + tableName + "'";
			
			rs = excuteSql(con, sql);
			if (rs.next()) {
				tableName = rs.getString("TABLE_NAME");
			}
			rs.close();
			rs = null;
			return userName + "." + tableName;
		}
		return table;
	}
}
