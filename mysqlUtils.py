import pyodbc
from connectionUtils import Connection
import string 

class MYSQLConnection(Connection):
	def __init__(self,  targetDriver="iLearnFederated", targetDB = "fed"):
		super().__init__(targetDriver, targetDB)
		self.tables = [index[0] for index in self.getSQLResult("show tables")]

	def writeToExternalFile(self, tableName,columnData,  delimiter = "|"):
		
		"""
		- Netezza does not support batch insert statements :( ....
		- This function writes the result of a mysql query to disk so that a batch upload can be completed
		"""
		sql = "SELECT * FROM " + tableName

		data = self.getSQLResult(sql)
		
		def fixEscapeChars(inputChr):
			if(inputChr == "\\"):
				return "\\" + inputChr
			elif(inputChr == "\""):
				return "\\" + inputChr
			elif(inputChr == "|"):
				return "\\" + inputChr
			else:
				return inputChr

		clearNonPrintables = lambda x : ''.join(fixEscapeChars(s) for s in x if (ord(s) >= 32 and ord(s) < 126))
			
		with open(tableName + ".dsv", 'a') as file:
			file.write(columnData + "\n")
		
		
		with open(tableName + ".dsv", 'a') as file:
			for row in data:
				cleanedRow = [clearNonPrintables(str(col)) for col in row]
				file.write(delimiter.join(cleanedRow))
				file.write("\n")
			