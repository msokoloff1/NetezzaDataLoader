import pyodbc

class Connection():
	def __init__(self, targetDriver,targetDB):
		self.conn = pyodbc.connect(dsn=targetDriver)
		self.cursor = self.conn.cursor()
		self.targetDB = targetDB
		
	def getSQLResult(self, statement):
		results = self.cursor.execute(statement)
		return results.fetchall()

	def executeSQL(self, statement):
		self.cursor.execute(statement)
		self.conn.commit()

	def getTables(self):
		return self.tables

