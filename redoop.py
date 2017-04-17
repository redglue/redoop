#!/usr/bin/env python
#
# Hadoop Data Advisor
# version 0.1
# Author: Redglue

import cx_Oracle
import sys
import getopt
import ConfigParser
import HTML
#import plotly.tools as tls
#import plotly.plotly as py
#from plotly.tools import FigureFactory as FF

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

		#add column description
		for c, col in enumerate(cursor.description):
			header.append(col[0])

		#add result set
		data=[list(row) for row in cursor]
		data.insert(0, header)

		return data
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
	print 'Example: redoop.py -b oracle -o advisor_result.html'



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

def OracleWorkflowAnalysisCMD(filename):
	f=openHTML(filename)
	print '## Hadoop Data Advisor - v0.1 (alpha) ##'
	con = connectOracle()
	raw_input("[1] Analyzing Workload - Press any key to continue...")
	print '[1.1] Analyzing Workload - Full Table Scans'
	q=readSQL('oracle/ftstables.sql')
	fts=executeQueryOracle(con, q)
	header=fts[0] # column names
	fts.pop(0)
	htmlcode = HTML.table(fts,header_row=header)
	writeHTML(f, htmlcode)
	#table = FF.create_table(fts, index=True)
	#py.plot(table, filename='Full Table Scans')
	print '[1.2] Analyzing Workload - Tables subject to modifications'
	q=readSQL('oracle/mostdml.sql')
	mdml=executeQueryOracle(con, q)
	header=mdml[0]
	mdml.pop(0)
	htmlcode = HTML.table(mdml, header_row=header)
	writeHTML(f, htmlcode)
	print '[1.3] Analyzing Workload - Range Partitioned Tables - Cold partitions'
	q=readSQL('oracle/rangetcold.sql')
	rangep=executeQueryOracle(con, q)
	header=rangep[0]
	rangep.pop(0)
	htmlcode = HTML.table(rangep, header_row=header)
	writeHTML(f, htmlcode)

	closeHTML(f)


def openHTML(filename):
	try:
		f = open(filename, 'w')
		return f
	except:
		raise

def closeHTML(f):
	try:
		f.close()
	except:
		raise

def writeHTML(f, htmlcode):
	try:
		f.write(htmlcode)
		f.write('<p>')
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


if __name__ == "__main__":
	main(sys.argv[1:])
