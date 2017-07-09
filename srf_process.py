#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict


def uploadSRF(conn=None, srf_path='/Users/chenxu/Work/Data/Futures/SRF_20170111.csv', \
	fields=['symbol', 'date', 'open', 'high', 'low', 'settle', 'vol', 'opi']):

	SRF_EXCHANGES = {
		'CBOT'  : 'CME',
		'NYMEX' : 'CME',
		'CME'   : 'CME',
		'ICE'   : 'ICE'
	}

	if not os.path.exists(srf_path):
		print("SRF file {0} doesn't exist, exit!".format(srf_path))
		sys.exit(1)

	print("Loading SRF data file {0} with fields {1} ...".format(srf_path, ','.join(fields)))
	srf = pd.read_csv(srf_path, parse_dates=[1], index_col=[0,1], header=None, names=fields)

	print("Loading table PRODUCTS ...")
	srf_products = pd.read_sql('select id, symbol, exchange from products where source = \'SRF\'', con=conn)
	srf_products.set_index(['symbol'], inplace=True)

	cur = conn.cursor()

	unregistered = defaultdict(bool)
	wrongs       = defaultdict(bool)
	skipped      = defaultdict(bool)

	frames = []
	
	for sym, df in srf.groupby(level="symbol"):
		df2 = df.xs(sym, level="symbol")

		(exch, sym_exp) = sym.split('_')
		match = re.search("^(\w+)([F-Z])(\d{4})$", sym_exp)

		if match:
			root = match.group(1)
			expM = match.group(2)
			year = match.group(3)

			if root in srf_products.index:
				pid 		  = srf_products.loc[root, "id"]
				exchange   = srf_products.loc[root, "exchange"]
				expiration = "{0}{1}".format(year, expM)

				"""
				if pid == 4007 and expiration == '2016H':
					print("############ Break: {0}  {1}  {2}  {3}  {4}".format(sym, root, expiration, exchange, exch))
					break;
				"""

				""" There both CME and ICE MP in SRF data """
				if exchange in SRF_EXCHANGES and SRF_EXCHANGES[exchange] != exch: 
					skipped[sym_exp] = True
					print("Skip {0} {1} {2} from exchange {3} ({4}) ...".format(\
						pid, root, expiration, exchange, exch))
					continue

				print("Reading {0} {1} {2} from exchange {3} ({4}) ...".format(\
					pid, root, expiration, exchange, exch))

				sql = pd.DataFrame(index=df2.index)
				sql.loc[:, "id"]     = pid
				sql.loc[:, "exp"]    = expiration
				sql.loc[:, "chain"]  = 0
				sql.loc[:, "open"]   = df2["open"]
				sql.loc[:, "high"]   = df2["high"]
				sql.loc[:, "low"]    = df2["low"]
				sql.loc[:, "close"]  = df2["settle"]
				sql.loc[:, "settle"] = df2["settle"]
				sql.loc[:, "vol"]    = df2["vol"]
				sql.loc[:, "opi"]    = df2["opi"]
				sql.loc[:, "pnl"]    = 0
				sql.loc[:, "logr"]   = 0

				sql.reset_index(inplace=True)
				frames.append(sql)
			else:
				print("Symbol {0} in {1} is not registered!".format(root, sym_exp))
				unregistered[root] = True
		else:
			print("Wrong symbol: {0}".format(sym_exp))	
			wrongs[sym] = True

	print("Finished inserting successfully!")

	if len(unregistered) > 0:
		print("\nUnregistered symbols: {0}\n".format(','.join(sorted(unergistered.keys()))))

	if len(wrongs) > 0:
		print("\nWrong symbols: {0}\n".format(','.join(sorted(wrongs.keys()))))

	if len(skipped) > 0:
		print("\nSkipped symbols: {0}\n".format(','.join(sorted(skipped.keys()))))


	""" Create a temporary CSV file for copying to the database """
	results = pd.concat(frames)
	sql_file = "sql_srf.csv"
	print("Saving to CSV file {0} ...".format(sql_file))
	results.to_csv(sql_file, sep="\t", header=False, na_rep="\\N", index=False,\
			columns=[
			"date",
			"id", 
			"exp", 
			"chain",
			"open", 
			"high", 
			"low", 
			"close", 
			"settle", 
			"vol", 
			"opi", 
			"pnl",
			"logr"])

	try:
		io = open(sql_file, "r")
		print("Truncating daily_data table ...")
		cur.execute('truncate table daily_data', conn)
		print("Copying to daily_data table ...")
		cur.copy_from(io, "daily_data")
		io.close()
	except IOError as e:
		errno, strerror = e.args
		print("I/O error ({0}) : {1}".format(errno, strerror))
	except ValueError:
		print("No valid integer in line.")
	except:
		print("Unexpected error: ", sys.exc_info()[0])
		raise

	cur.close()

	return results



if __name__ == "__main__":
	
	connection = pg.connect(host="localhost", database="futures", user="chenxu", password="")
	res = uploadSRF(connection)

	print("Committing changes to the database ...")
	connection.commit()
	connection.close()

