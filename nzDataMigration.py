from nzUtils import NZConnection
from mysqlUtils import MYSQLConnection



def main(tablesSelected, message, tableList):
	tablePrefix = ""
	while(True):
		if(len(tablesSelected) > 0):
			print("Tables already selected : ")
			print(tablesSelected)

		#For python 2
		#var = raw_input(message)
		var = input(message)
		if(var.lower() == "all"):
			tablesSelected = tableList
		elif(var.lower() == "go"):
			execute(tablesSelected,tablePrefix)
		elif(var.lower() == "quit" or var.lower() == "exit"):
			print("Quitting")
			exit(0)
		elif(isInt(var)):
			try:
				tablesSelected.append(tableList[int(var)])
				print(tableList[int(var)]+ " added to migration list")
			except:
				print("unknown index, failed to add table")
		elif(var.tolower() == "prefix"):
			tablePrefix = input("Please enter the prefix you would like\n =>")
		else:
			print("Unknown input, please try again.")

def listTables(tables):
	"""
	Prints a list of all tables available in the source table
	"""
	print("Tables and their corresponding Indices")
	for key, value in enumerate(tables):
		print("Index : " + str(key).zfill(3) + " --- Table : " + str(value))


def execute(tablesSelected, tablePrefix):
	"""
	Runs the migration after settings have been configured
	"""
	if(len(tablesSelected) == 0):
			print("Error, you haven't selected any tables... exiting")
			exit(0)
	else:
		errors, total = nz.populateFromMYSQL(mysql, tablesSelected, tablePrefix)
		print("Migration Finished:\n"
			+"Errors - " + str(erros)
			+"\nTotal - " + str(total)
		)
		exit(0)


def isInt(string):
	"""
	Determines whether or not the input is a number
	"""
	try:
		float(string)
		return True
	except:
		return False


nz = NZConnection()
print("NETEZZA CONNECTION")
mysql = MYSQLConnection()
print("MYSQL CONNECTION")

var = ""
message = "\nPlease enter one of the following commands : \n\
1. Numeric table index to add a table to list.\n\
2. \"all\" to migrate all tables from mysql. \n\
3. \"go\" to execute the migration for all current tables listed\n\
4. \"quit\" or \"exit\" to cancel the data migration\n\
5. \"ls\" to list all tables available in the source mysql database\n\
6. \"prefix\" to change the table prefix to be used\n\n=>"

tableList = mysql.getTables()
listTables(tableList)
tablesSelected = []

main(tablesSelected, message, tableList)