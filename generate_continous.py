#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict

from datetime import datetime

def generateHDF(conn=None, sql_begin='2015-01-01', ch=1, h5='continuous_chain', refresh=False,
	fields=['open', 'high', 'low', 'close', 'settle', 'vol', 'opi', 'pnl', 'logr']):

	print("Loading products data ...")
	products = pd.read_sql("select id, symbol, exchange, category from products", conn)
	products.set_index('id', inplace=True)

	print("Loading daily data ...")
	dailydata = pd.read_sql(\
		"select date,id,{0} from daily_data where date >= \'{1}\' and chain = {2}".format(\
		','.join(fields), sql_begin, ch), conn)

	print("Resetting index ...")
	dailydata.set_index(['id', 'date'], inplace=True)
	print("Sorting index ...")
	dailydata.sort_index(level=['id', 'date'], inplace=True)

	frames = defaultdict(pd.DataFrame)

	for pid, df in dailydata.groupby(level='id'):
		df2 = df.xs(pid, level='id')
		sym = products.ix[pid, 'symbol']

		print("Creating frames for chain {0} {1} {2} ...".format(ch, pid, sym))
		for fld in fields:
			df3 = df2[fld].to_frame(name=sym)
			frames[fld] = pd.merge(frames[fld], df3, how='outer', left_index=True, right_index=True)

	h5 = "{0}_{1}.h5".format(h5, ch)

	if refresh and os.path.exists(h5):
		print("Deleting {0} ...".format(h5))
		os.remove(h5)

	print("Creating file {0} ...".format(h5))
	products.to_hdf(h5, "products", mode='a')
	for fld in frames.keys():
		frames[fld].to_hdf(h5, "daily{0}".format(fld), mode='a')
		frames[fld].to_csv("daily{0}.csv".format(fld))

	return frames


def generateWikiEquityHDF(wiki, refresh=True, generateCSV=False):
	print("Loading data {0} ...".format(wiki))
	data = pd.read_csv(wiki, delimiter=',', parse_dates=[1], index_col=[0, 1], header=None, skiprows=1,
		names=[\
			'ticker', 
			'date', 
			'open', 
			'high', 
			'low', 
			'close', 
			'volume', 
			'div', 
			'split', 
			'adjopen', 
			'adjhigh', 
			'adjlow', 
			'adjclose', 
			'adjvol'\
		], na_values=["", "NaN", "nan"])
	data.sort_index(level=['ticker', 'date'], inplace=True)

	frames = defaultdict(pd.DataFrame)
	for ticker, df in data.groupby(level='ticker'):
		df2 = df.xs(ticker, level='ticker')

		print("{0}".format(ticker))
		for fld in data.columns:
			df3 = df2[fld].to_frame(name=ticker)
			frames[fld] = pd.merge(frames[fld], df3, how='outer', left_index=True, right_index=True)

	h5 = "WikiEOD.h5"
	if refresh and os.path.exists(h5):
		print("Deleting {0} ...".format(h5))
		os.remove(h5)

	print("Creating file {0} ...".format(h5))
	for fld in frames.keys():
		frames[fld].to_hdf(h5, "{0}".format(fld), mode='a')
		if generateCSV:
			frames[fld].to_csv("WikiEOD_{0}.csv".format(fld))

	return frames



if __name__ == "__main__":
	
	connection = pg.connect(host="localhost", database="futures", user="chenxu", password="")

#	for ch in range(1, 3):
#		frames = generateHDF(connection, '2000-01-01', ch, 'continuous_chain', True)

	connection.close()

	wiki = "/Users/chenxu/Work/Data/Equity/WikiEOD/WIKI_PRICES_212b326a081eacca455e13140d7bb9db.csv"
	stocks = generateWikiEquityHDF(wiki, True, False)

