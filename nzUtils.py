import pyodbc
from connectionUtils import Connection
import re

class NZConnection(Connection):
	def __init__(self, targetDriver="NZSQL", targetDB ="databaseName"):
		super().__init__(targetDriver, targetDB)
		self.tables = [index[0] for index in self.getSQLResult("SELECT TABLENAME FROM "+targetDB+"._V_TABLE")]

		self.mysqlDataTypes = set([])
		self.dataLookup = {
			  'varchar'    : 'CHARACTER VARYING('
			, 'mediumtext' : 'CHARACTER VARYING(16000)'
			, 'date'       : 'CHARACTER VARYING(128)'
			, 'bigint'     : 'CHARACTER VARYING(128)' #'BIGINT' #<= number will be added (ie 'BIGINT34)' ) need to fix
			, 'bit'        : 'CHARACTER VARYING(128)'
			, 'int'        : 'CHARACTER VARYING(128)'
			, 'double'     : 'CHARACTER VARYING(128)'
			, 'tinyint'    : 'CHARACTER VARYING(10)'
			, 'text'       : 'CHARACTER VARYING(16000)'
			, 'timestamp'  : 'CHARACTER VARYING(128)'
		}

		#, 'datetime'   : 'TIMESTAMP' #<= not sure about this one
	def populateFromMYSQL(self, mysqlConnection, tableList, prefix):
		"""
		arg mysqlConnection : An instantiated MYSQLConnection object
		arg tableList       : A python list of tables from the source database
		arg prefix          : The prefix to be added to the table name in the netezza destination database
		"""
		errors = 0
		count = 0

		tableData = lambda table: mysqlConnection.getSQLResult("show columns from " + table)
		for table in tableList:
			#Extra queries.. change once working
			columnData = [str(result[0])+" "+self._cleanDataType(result[1]) for result in tableData(table)]
			columnNames = [str(result[0]) for result in tableData(table)]

			#creates dsv file
			mysqlConnection.writeToExternalFile(table, "|".join(columnData))

			

			prefix = "testingImport"
			ddlScript = self._generateCreate(prefix, table, columnData)
			insertScript = self._generateInsert(prefix, table, columnNames, columnData)


			try:
				self.executeSQL(ddlScript)
				print("DDL EXECUTED")
			except:
				errors += 1
				print("Unable to create " + table)
			print("TRYING:")
			try:
				self.executeSQL(insertScript)
				print(insertScript)
				print("INSERT EXECUTED")
			except:
				print("Unable to populate " + table)
				
			count+= 1
			
		return [errors, count]

	def _generateCreate(self, prefix, table, columnData):
		ddlScript = "CREATE TABLE IF NOT EXISTS \"" + prefix+"_" + table + "\"" \
						+ " ( " \
						+ 	",".join(columnData) \
						+ " ) DISTRIBUTE ON RANDOM"
		return ddlScript

	def _generateInsert(self, prefix, table, columnNames, columnData):
		insertScript = "INSERT INTO \"" + prefix + "_" + table + "\"" \
						+ " ( " \
						+ ",".join(columnNames) \
						+ " ) " \
						+ " SELECT * FROM EXTERNAL '" \
						+ table + ".dsv'" \
						+ " ( " \
						+ ",".join(columnData) \
						+ " ) " \
						+ " USING " \
						+ " ( " \
						+    "NULLVALUE '' " \
						+    "CRINSTRING TRUE " \
						+    "REMOTESOURCE 'ODBC' " \
						+    "ESCAPECHAR '\\' " \
						+    "DELIMITER '|' "   \
						+ " )"
						#+    "QUOTEDVALUE 'DOUBLE' " \
		return insertScript

	def _cleanDataType(self, dataType):
		self.mysqlDataTypes.add(dataType) #<= contains distinct inbound datatypes
		for mysqlDataType in self.dataLookup:
			if(bool(re.match(mysqlDataType + ".*",str(dataType).lower()))):
				result = self.dataLookup[mysqlDataType]
				if (result == 'CHARACTER VARYING('):
					dataSize = re.findall(r'\d+', dataType)
					result += str(int(dataSize[0]) + 100 ) + ")"

				return result

		assert "MySQL datatype does not have corresponding NZSQL datatype"
