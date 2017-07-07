#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict


def upload(conn=None, srf_path="/Users/chenxu/Work/Data/Futures/SRF_20170111.csv", \
	fields=["symbol", "date", "open", "high", "low", "settle", "vol", "opi"]):

	if not os.path.exists(srf_path):
		print("SRF file {0} doesn't exist, exit!".format(srf_path))
		sys.exit(1)

	print("Loading SRF data file {0} with fields {1} ...".format(srf_path, ','.join(fields)))
	srf = pd.read_csv(srf_path, parse_dates=[1], index_col=[0,1], header=None, names=fields)

	print("Loading table SRF_PRODUCTS ...")
	srf_products = pd.read_sql('select id, symbol, exchange from srf_products', con=conn)
	srf_products.set_index(["symbol"], inplace=True)

	cur = conn.cursor()

	unregistered = defaultdict(bool)
	wrongs = defaultdict(bool)
	
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

				if exchange == "ICE" and exch != exchange: continue

				print("Reading {0} {1} {2}".format(pid, root, expiration))
				sql = pd.DataFrame(index=df2.index)
				sql.loc[:, "id"]  = pid
				sql.loc[:, "exp"] = expiration
				sql.loc[:, "vol"] = df2["vol"]
				sql.loc[:, "opi"] = df2["opi"]

				op = df2["open"].copy(deep=True)
				hi = df2["high"].copy(deep=True)
				lo = df2["low"].copy(deep=True)
				st = df2["settle"].copy(deep=True)

				op.fillna(method="ffill", inplace=True)
				hi.fillna(method="ffill", inplace=True)
				lo.fillna(method="ffill", inplace=True)
				st.fillna(method="ffill", inplace=True)

				sql.loc[:, "open"] 	 = op
				sql.loc[:, "high"] 	 = hi
				sql.loc[:, "low"]  	 = lo
				sql.loc[:, "close"]   = st
				sql.loc[:, "settle"]  = st

				sql.loc[:, "pnl"]     = st.pct_change()
				sql.loc[:, "logr"]    = np.log(st).diff()

				sql["vol"].fillna(0, inplace=True)
				sql["opi"].fillna(0, inplace=True)
				sql["pnl"].fillna(0, inplace=True)
				sql["logr"].fillna(0, inplace=True)

				sql.reset_index(inplace=True)

				sql_file = "srf_sql.tmp"
				sql.to_csv(sql_file, sep="\t", header=False, na_rep="\\N", index=False,\
						columns=[
						"date",
						"id", 
						"exp", 
						"open", 
						"high", 
						"low", 
						"close", 
						"settle", 
						"vol", 
						"opi", 
						"pnl",
						"logr"])

				io = open(sql_file, "r")
				cur.copy_from(io, "srf_data")
				io.close()
				os.remove(sql_file)

			else:
				print("Symbol {0} in {1} is not registered!".format(root, sym_exp))
				unregistered[root] = True
		else:
			print("Wrong symbol: {0}".format(sym_exp))	
			wrongs[sym] = True

	print("Finished inserting successfully!")

	if len(unregistered) > 0:
		print("\nUnregistered symbols:\n")
		for sym in sorted(unregistered.keys()):
			print(sym)

	if len(wrongs) > 0:
		print("\nWrong symbols:\n")
		for sym in sorted(wrongs.keys()):
			print(sym)

	cur.close()


if __name__ == "__main__":
	
	connection = pg.connect(host="localhost", database="futures", user="chenxu", password="postgres")
	upload(connection)

	connection.commit()
	connection.close()

