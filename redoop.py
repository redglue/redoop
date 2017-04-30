#!/usr/bin/env python
#
# Hadoop Data Advisor
# version 0.2
# Author: Redglue (Luís Marques luis.marques@redglue.eu)

import cx_Oracle
import sys
import getopt
import ConfigParser
from tabulate import tabulate
import datetime
import socket


#### ORACLE BASIC OPERATIONS ####

def readOracleCS():
	try:
		connect_string = ConfigSectionMap("oracle")['connection_string']
 		return connect_string
 	except:
 		print 'E: Error reading Oracle configuration.'
		sys.exit(1)

def connectOracle():
	try:
		connect_string = readOracleCS()
		con = cx_Oracle.connect(connect_string)
		return con
	except cx_Oracle.DatabaseError as e:
		raise
		print 'E: Error connectiong to database.'
		sys.exit(1)


def executeQueryOracle(con, query):
	try:
		header=[]
		cursor = con.cursor()
		result=cursor.execute(query)
		head=cursor.description

		#add column description
		for c, col in enumerate(cursor.description):
			header.append(col[0])

		#add result set
		data=[list(row) for row in cursor]
		data.insert(0, header)

		return data
	except:
		print 'E: Error running Oracle query.'
		sys.exit(1)


def readSQL(filename):
	try:
		q=open(filename, 'r')
		return q.read()
		q.close()

	except:
		print 'E: Error reading configuration file.'
		sys.exit(1)



def help():

	print 'redoop.py -d <database> -o <output_file>'
	print 'Example: redoop.py -b oracle -o file.txt'



def cmdlineOpts():

	filename=''
	database=''
	try:
		opts, args = getopt.getopt(sys.argv[1:], 'hd:o:')
	except getopt.GetoptError as err:
		print(err)
		sys.exit()
	for o,a in opts:
		if o in ("-d"):
			database=a
		elif o in ("-o"):
			filename=a
		elif o in ("-h"):
			 help()
			 sys.exit()
		else:
			help()
			sys.exit()

	return database, filename


def ConfigSectionMap(section):
	dict1 = {}
	Config = ConfigParser.ConfigParser()
	Config.read('config.ini')
	options = Config.options(section)
	for option in options:
		try:
			dict1[option] = Config.get(section, option)
			if dict1[option] == -1:
				DebugPrint("skip: %s" % option)
		except:
			print("E: Exception on reading  %s!" % option)
			dict1[option] = None
	return dict1

def getToday():
	return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def getHostname():
	return socket.gethostname()

def OracleWorkflowAnalysisCMD(filename):
	f=openFile(filename)
	con = connectOracle()
	writeFile(f, 'redoop - Oracle Offload Hadoop Advisor - Report')
	timenow=getToday()
	hostname=getHostname()
	connectionString=readOracleCS()
	writeFile(f, 'Date: '+ timenow)
	writeFile(f, 'Client hostname: '+hostname)
	writeFile(f, 'Connection String: '+connectionString+'\n')
	print '## Hadoop Data Advisor - v0.1 (alpha) ##'

	raw_input("[1] Analyzing Workload - Press any key to continue...")
	print '[1.1] Analyzing Workload - Full Table Scans'
	q=readSQL('oracle/ftstables.sql')
	fts=executeQueryOracle(con, q)
	header=fts[0] # column names
	writeFile(f, '== Full Table Scans ==')
	writeFile(f, tabulate(fts, headers="firstrow")+'\n')

	print '[1.2] Analyzing Workload - Tables subject to modifications'
	q=readSQL('oracle/mostdml.sql')
	mdml=executeQueryOracle(con, q)
	header=mdml[0]
	writeFile(f, '== Tables subject to modifications ==')
	writeFile(f, tabulate(mdml,headers="firstrow")+'\n')

	print '[1.3] Analyzing Workload - Range Partitioned Tables - Cold partitions'
	q=readSQL('oracle/rangetcold.sql')
	rangep=executeQueryOracle(con, q)
	header=rangep[0]
	writeFile(f, '== Tables subject to modifications ==')
	writeFile(f, tabulate(rangep, headers="firstrow")+'\n')

	closeFile(f)

def openFile(filename):
	try:
		f = open(filename, 'w')
		return f
	except:
		raise

def closeFile(f):
	try:
		f.close()
	except:
		raise

def writeFile(f, output):
	try:
		f.write(output)
		f.write('\n')
	except:
		raise

def main(argv):
	#tls.set_credentials_file(username='luis.marques', api_key='BMpB7AHKGNMVWSdFjxHg')
	database, filename = cmdlineOpts()
	print database, filename
	if database == 'oracle':
		OracleWorkflowAnalysisCMD(filename)
	else:
		print 'E: Database not supported!'
		sys.exit(1)


if __name__ == "__main__":
	main(sys.argv[1:])
