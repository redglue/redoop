#!/usr/bin/env python
#                                
# Hadoop Data Advisor
# version 0.1
# Author: Redglue

import cx_Oracle
import sys
import getopt
import ConfigParser
import plotly.tools as tls
import plotly.plotly as py
from plotly.tools import FigureFactory as FF 

#### ORACLE BASIC OPERATIONS ####

def readOracleCS():
	try:
		connect_string = ConfigSectionMap("oracle")['connection_string']
 		return connect_string
 	except:
 		raise

def connectOracle():
	try:
		connect_string = readOracleCS()
		con = cx_Oracle.connect(connect_string)
		return con
	except cx_Oracle.DatabaseError as e:
		raise

def executeQueryOracle(con, query):
	try:
		header=[]
		cursor = con.cursor()
		result=cursor.execute(query)
		head=cursor.description
		
		for i in head: header.append(i[0])

		data=[list(row) for row in cursor]
		
		data.insert(0, header)

		

	except:
		raise


def readSQL(filename):
	try:
		q=open(filename, 'r')
		return q.read()
		q.close()

	except:
		raise



def help():
	
	print 'redoop.py -b <database> -o <output_file>'
	print 'Example: hda.py -b oracle -o advisor_result.html'



def cmdlineOpts():

	database='oracle'
	filename='advisor.html'

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

def OracleWorkflowAnalysisCMD():

	print '## Hadoop Data Advisor - v0.1 (alpha) ##'
	con = connectOracle()
	raw_input("[1] Analyzing Workload - Press any key to continue...")
	print '[1.1] Analyzing Workload - Full Table Scans'
	q=readSQL('oracle/ftstables.sql')
	fts=executeQueryOracle(con, q)
	table = FF.create_table(fts)
	py.plot(table, filename='Full Table Scans')
	print '[1.2] Analyzing Workload - Tables subject to modifications'
		 


def main(argv):
	tls.set_credentials_file(username='luis.marques', api_key='BMpB7AHKGNMVWSdFjxHg')
	database, filename = cmdlineOpts()
	OracleWorkflowAnalysisCMD()


if __name__ == "__main__":
	main(sys.argv[1:])
