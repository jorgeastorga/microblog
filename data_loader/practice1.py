__author__ = 'jorge.astorga'
from sqlalchemy import create_engine,MetaData,Table,Column,Float,Date,Integer,String
from datetime import datetime

#Constructs a transaction array list
def getTransactionsList(transactionList):
	for line in fileHandle:
		#Cleanup some of the data in CSV that comes from Fidelity
		line = line.rstrip()
		if 'Plan name' in line:
			continue
		elif 'Date Range' in line:
			continue
		elif line == "":
			continue
		elif 'Date' in line:
			continue

		transactionElements = line.split(',')

		#Convert date string into datetime object
		dateString = transactionElements[0]		
		dateObject = datetime.strptime(dateString, '%m/%d/%Y')

		#Mapping transaction elements to meaninful names
		symbol = transactionElements[1]
		transactionType = transactionElements[2]
		totalAmountPurchased = transactionElements[3]
		pricePerShare = transactionElements[4]

		#Construct transactions list (array)
		transactionElementsArray = list([dateObject, 
			symbol,
			transactionType,
			float(totalAmountPurchased.strip('\"')),
			float(pricePerShare.strip('\"'))]) #removed the extra double quotes

		transactionList.append(transactionElementsArray)


def getTransactionsByFund(transactionsByFund):
	
	for line in fileHandle:
		line = line.rstrip()
		if 'Plan name' in line:
			continue
		elif 'Date Range' in line:
			continue
		elif line == "":
			continue
		elif 'Date' in line:
			continue
		#print line

		transactionComponents = line.split(',')

		newComponents = list([transactionComponents[0], 
			transactionComponents[2],
			transactionComponents[3],
			transactionComponents[4]])

		fundName =transactionComponents[1]

		if fundName not in transactionsByFund.keys():
			transactionsByFund[fundName] = list()
			transactionsByFund[fundName].append(newComponents)
		else:
			transactionsByFund[fundName].append(newComponents)



def printTransactions(transactionsList):
	for key in transactionsList.keys():
		print (key)

def pricePerShareHistory(fundName):
	#get list of transactions for a specific fund
	#iterate through the list of transactions for each day, print the price per share for reach day
	transactionList = transactionsByFund[fundName]
	for transaction in transactionList:
		print transaction

#Method used to populate transaction data into the database
def populateDatabase(transactionsTable, transactionList):
	
	engine = create_engine('sqlite:///data.db', echo=True)

	for transaction in transactionsList:
		ins = transactionsTable.insert().values(purchasedate=transaction[0],
			contributiontype=transaction[2],
			shareprice=transaction[4],
			purchasetotal=transaction[3],
			symbol=transaction[1])

		print(ins)
		print(ins.compile().params)

		conn = engine.connect()
		result = conn.execute(ins)



def createDatabase():
	
	engine = create_engine('sqlite:///data.db', echo=True)

	metaData = MetaData()

	transactionsTable = Table('transactions', metaData, 
		Column('id', Integer, primary_key=True), 
		Column('symbol', String),
		Column('contributiontype', String),
		Column('shareprice', Float),
		Column('purchasetotal', Float),
		Column('purchasedate', Date))

	metaData.create_all(engine)

	print('Database created successfully')
	return transactionsTable


fileHandle = open('history.csv')
SQLITE_DB_PATH = 'sqlite:///data.db'

transactionsByFund = dict()
transactionsList = list()

#database engine
metaData = None
transactionsTable = None


getTransactionsList(transactionsList)
transactionsTable = createDatabase()
populateDatabase(transactionsTable,transactionsList)
#getTransactionsByFund(transactionsByFund)
#printTransactions(transactionsByFund)


	