import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class ReadMetaData {
	public Connection con;
	final static Logger log = Logger.getLogger(ReadMetaData.class);

	public ReadMetaData() throws SQLException {
		con = DriverManager.getConnection(
				"jdbc:db2://dashdb-txn-sbox-yp-dal09-11.services.dal.bluemix.net:50001/BLUDB:user=rkh10014;password=rvlg2s8g0143-k8h;sslConnection=true;");
	}

	public List<String> getSchemaData(BufferedWriter bw) throws SQLException, IOException {
		log.info("--Get SCHEMAS--");
		List<String> schemas = new ArrayList<>();
		PreparedStatement ps = con.prepareStatement("SELECT * FROM SYSCAT.SCHEMATA WHERE OWNERTYPE = 'U'");
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			schemas.add(rs.getString(1).trim());
		}
		rs.close();
		bw.write("--SCHEMAS--");
		bw.newLine();
		printData(ps.executeQuery(), bw);
		return schemas;
	}

	public void processMetaData(String schemaName, BufferedWriter bw) throws Exception {

		ResultSet rs;

		bw.write("--TABLES-- :: " + schemaName);
		bw.newLine();
		log.info("--TABLES-- :: " + schemaName);
		System.out.println("--TABLES-- :: " + schemaName);
		PreparedStatement tableMeta = con.prepareStatement("SELECT * FROM SYSCAT.TABLES WHERE TABSCHEMA = ?");
		tableMeta.setString(1, schemaName);
		rs = tableMeta.executeQuery();
		printData(rs, bw);

		bw.write("--TABLES DEP-- :: " + schemaName);
		bw.newLine();
		log.info("--TABLES DEP-- :: " + schemaName);
		System.out.println("--TABLES DEP-- :: " + schemaName);
		PreparedStatement tableDepMeta = con.prepareStatement("SELECT * FROM SYSCAT.TABDEP WHERE owner = ?");
		tableDepMeta.setString(1, schemaName);
		rs = tableDepMeta.executeQuery();
		printData(rs, bw);

		bw.write("--TABLE COLUMNS-- :: " + schemaName);
		bw.newLine();
		log.info("--TABLE COLUMNS-- :: " + schemaName);
		System.out.println("--TABLE COLUMNS-- :: " + schemaName); // Do we need table wise ??
		PreparedStatement columnMeta = con.prepareStatement("SELECT * FROM SYSCAT.COLUMNS WHERE TABSCHEMA = ?");
		columnMeta.setString(1, schemaName);
		rs = columnMeta.executeQuery();
		printData(rs, bw);

		bw.write("--UNIQUE KEY CONSTRAINTS-- :: " + schemaName);
		bw.newLine();
		log.info("--UNIQUE KEY CONSTRAINTS-- :: " + schemaName);
		System.out.println("--UNIQUE KEY CONSTRAINTS-- :: " + schemaName);
		PreparedStatement uniqKeyMeta = con.prepareStatement(
				"SELECT C.*, KCU.COLNAME, KCU.COLSEQ FROM SYSCAT.TABCONST C, SYSCAT.KEYCOLUSE KCU WHERE C.TABSCHEMA = ? "
						+ "AND C.TYPE IN ('P','U') AND KCU.CONSTNAME = C.CONSTNAME AND KCU.TABSCHEMA = C.TABSCHEMA AND KCU.TABNAME = C.TABNAME "
						+ "ORDER BY C.CONSTNAME, KCU.COLSEQ WITH UR");
		uniqKeyMeta.setString(1, schemaName);
		rs = uniqKeyMeta.executeQuery();
		printData(rs, bw);

		bw.write("--FOREIGN KEY CONSTRAINTS-- :: " + schemaName);
		bw.newLine();
		log.info("--FOREIGN KEY CONSTRAINTS-- :: " + schemaName);
		System.out.println("--FOREIGN KEY CONSTRAINTS-- :: " + schemaName);
		PreparedStatement foreignKeyMeta = con.prepareStatement(
				"SELECT R.*, KCU.COLNAME, KCU.COLSEQ FROM SYSCAT.REFERENCES R, SYSCAT.KEYCOLUSE KCU WHERE R.TABSCHEMA = ? "
						+ "AND KCU.CONSTNAME = R.CONSTNAME AND KCU.TABSCHEMA = R.TABSCHEMA  AND KCU.TABNAME = R.TABNAME "
						+ "ORDER BY R.CONSTNAME, KCU.COLSEQ WITH UR");
		foreignKeyMeta.setString(1, schemaName);
		rs = foreignKeyMeta.executeQuery();
		printData(rs, bw);

		bw.write("--INDEXES-- :: " + schemaName);
		bw.newLine();
		log.info("--INDEXES-- :: " + schemaName);
		System.out.println("--INDEXES-- :: " + schemaName);
		PreparedStatement indexMeta = con.prepareStatement("SELECT * FROM SYSCAT.INDEXES WHERE INDSCHEMA = ?");
		indexMeta.setString(1, schemaName);
		rs = indexMeta.executeQuery();
		printData(rs, bw);

		bw.write("--INDEX COLUMNS-- :: " + schemaName);
		bw.newLine();
		log.info("--INDEX COLUMNS-- :: " + schemaName);
		System.out.println("--INDEX COLUMNS-- :: " + schemaName);
		PreparedStatement indexColMeta = con.prepareStatement("SELECT * FROM SYSCAT.INDEXCOLUSE WHERE INDSCHEMA = ?");
		indexColMeta.setString(1, schemaName);
		rs = indexColMeta.executeQuery();
		printData(rs, bw);

		bw.write("--REFERENCES-- :: " + schemaName);
		bw.newLine();
		log.info("--REFERENCES-- :: " + schemaName);
		System.out.println("--REFERENCES-- :: " + schemaName);
		PreparedStatement referencesMeta = con.prepareStatement(
				"SELECT R.*, KCU.COLNAME, KCU.COLSEQ FROM SYSCAT.REFERENCES R , SYSCAT.KEYCOLUSE KCU WHERE R.REFTABSCHEMA = ? "
						+ "AND KCU.CONSTNAME = R.REFKEYNAME AND KCU.TABSCHEMA = R.REFTABSCHEMA AND KCU.TABNAME = R.REFTABNAME "
						+ "ORDER BY R.REFKEYNAME, KCU.COLSEQ WITH UR");
		referencesMeta.setString(1, schemaName);
		rs = referencesMeta.executeQuery();
		printData(rs, bw);

		bw.write("--CHECK CONSTRAINTS-- :: " + schemaName);
		bw.newLine();
		log.info("--CHECK CONSTRAINTS-- :: " + schemaName);
		System.out.println("--CHECK CONSTRAINTS-- :: " + schemaName);
		PreparedStatement checksMeta = con.prepareStatement(
				"SELECT C.*, CK.COLNAME, CK.USAGE FROM SYSCAT.CHECKS C, SYSCAT.COLCHECKS CK WHERE C.TABSCHEMA = ? "
						+ "AND CK.CONSTNAME = C.CONSTNAME AND CK.TABSCHEMA = C.TABSCHEMA AND CK.TABNAME = C.TABNAME "
						+ "ORDER BY CK.COLNAME WITH UR");
		checksMeta.setString(1, schemaName);
		rs = checksMeta.executeQuery();
		printData(rs, bw);

		bw.write("--TRIGGERS-- :: " + schemaName);
		bw.newLine();
		log.info("--TRIGGERS-- :: " + schemaName);
		System.out.println("--TRIGGERS-- :: " + schemaName);
		PreparedStatement trgMeta = con.prepareStatement("SELECT * FROM SYSCAT.TRIGGERS WHERE TRIGSCHEMA = ?");
		trgMeta.setString(1, schemaName);
		rs = trgMeta.executeQuery();
		printData(rs, bw);

		bw.write("--TRIGGERS DEP-- :: " + schemaName);
		bw.newLine();
		log.info("--TRIGGERS DEP-- :: " + schemaName);
		System.out.println("--TRIGGERS DEP-- :: " + schemaName);
		PreparedStatement trgDepMeta = con.prepareStatement("SELECT * FROM SYSCAT.TRIGDEP WHERE TRIGSCHEMA = ?");
		trgDepMeta.setString(1, schemaName);
		rs = trgDepMeta.executeQuery();
		printData(rs, bw);

		bw.write("--VIEWS-- :: " + schemaName);
		bw.newLine();
		log.info("--VIEWS-- :: " + schemaName);
		System.out.println("--VIEWS-- :: " + schemaName);
		PreparedStatement viewMeta = con.prepareStatement("SELECT * FROM SYSCAT.VIEWS WHERE  VIEWSCHEMA = ?");
		viewMeta.setString(1, schemaName);
		rs = viewMeta.executeQuery();
		printData(rs, bw);

		bw.write("--SEQUENCES-- :: " + schemaName);
		bw.newLine();
		log.info("--SEQUENCES-- :: " + schemaName);
		System.out.println("--SEQUENCES-- :: " + schemaName);
		PreparedStatement seqMeta = con.prepareStatement("SELECT * FROM SYSCAT.SEQUENCES WHERE SEQSCHEMA = ?");
		seqMeta.setString(1, schemaName);
		rs = seqMeta.executeQuery();
		printData(rs, bw);

		bw.write("--ALIASES-- :: " + schemaName);
		bw.newLine();
		log.info("--ALIASES-- :: " + schemaName);
		System.out.println("--ALIASES-- :: " + schemaName);
		PreparedStatement aliasMeta = con
				.prepareStatement("SELECT *  FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TYPE = 'A'");
		aliasMeta.setString(1, schemaName);
		rs = aliasMeta.executeQuery();
		printData(rs, bw);

		bw.write("--NICKNAMES-- :: " + schemaName);
		bw.newLine();
		log.info("--NICKNAMES-- :: " + schemaName);
		System.out.println("--NICKNAMES-- :: " + schemaName);
		PreparedStatement nickMeta = con.prepareStatement("SELECT * FROM SYSCAT.NICKNAMES WHERE TABSCHEMA = ?");
		nickMeta.setString(1, schemaName);
		rs = nickMeta.executeQuery();
		printData(rs, bw);

		bw.write("--FUNCTIONS-- :: " + schemaName);
		bw.newLine();
		log.info("--FUNCTIONS-- :: " + schemaName);
		System.out.println("--FUNCTIONS-- :: " + schemaName);
		PreparedStatement funcMeta = con
				.prepareStatement("SELECT * FROM SYSCAT.ROUTINES WHERE ROUTINESCHEMA = ? AND ROUTINETYPE = 'F'");
		funcMeta.setString(1, schemaName);
		rs = funcMeta.executeQuery();
		printData(rs, bw);

		bw.write("--STORED PROCEDURES-- :: " + schemaName);
		bw.newLine();
		log.info("--STORED PROCEDURES-- :: " + schemaName);
		System.out.println("--STORED PROCEDURES-- :: " + schemaName);
		PreparedStatement procMeta = con
				.prepareStatement("SELECT * FROM SYSCAT.ROUTINES WHERE ROUTINESCHEMA = ? AND ROUTINETYPE = 'P'");
		procMeta.setString(1, schemaName);
		rs = procMeta.executeQuery();
		printData(rs, bw);

		bw.write("--MODULES-- :: " + schemaName);
		bw.newLine();
		log.info("--MODULES-- :: " + schemaName);
		System.out.println("--MODULES-- :: " + schemaName);
		PreparedStatement modMeta = con.prepareStatement("SELECT * FROM SYSCAT.MODULES WHERE MODULESCHEMA = ?");
		modMeta.setString(1, schemaName);
		rs = modMeta.executeQuery();
		printData(rs, bw);

		bw.write("--TABLE PRIVILEGES-- :: " + schemaName);
		bw.newLine();
		log.info("--TABLE PRIVILEGES-- :: " + schemaName);
		System.out.println("--TABLE PRIVILEGES-- :: " + schemaName);
		PreparedStatement privMeta = con.prepareStatement("SELECT * FROM  SYSIBM.SYSTABAUTH WHERE TCREATOR = ?");
		privMeta.setString(1, schemaName);
		rs = privMeta.executeQuery();
		printData(rs, bw);
	}

	public void printColumnHeaders(ResultSetMetaData res, int count, BufferedWriter bw) throws SQLException, IOException {
		List<String> list = new ArrayList<>();
		for (int i = 1; i <= count; i++) {
			list.add(res.getColumnName(i));
		}
		bw.write(String.join("|", list));
		bw.newLine();
	}

	public void printData(ResultSet res, BufferedWriter bw) throws SQLException, IOException {
		int count = res.getMetaData().getColumnCount();
		printColumnHeaders(res.getMetaData(), count, bw);
		List<String> list = new ArrayList<>();
		while (res.next()) {
			for (int i = 1; i <= count; i++) {
				list.add(res.getString(i));
			}
			bw.write(String.join("|", list));
			bw.newLine();
			list.clear();
		}
		res.close();
		bw.newLine();
	}

	public void printProductData(BufferedWriter bw) throws IOException, SQLException {
			log.info("In Product Data");
			DatabaseMetaData meta = con.getMetaData();
			bw.write("Database Name :: " + meta.getDatabaseProductName());
			bw.newLine();
			bw.write("Database Version :: " + meta.getDatabaseProductVersion());
			bw.newLine();
			System.out.println("Database Name :: " + meta.getDatabaseProductName());
			System.out.println("Database Version :: " + meta.getDatabaseProductVersion());
	}
}
