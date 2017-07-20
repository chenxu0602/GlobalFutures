#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict

def generateHDF(conn=None, sql_begin='2015-01-01', ch=1, h5='continuous_chain', refresh=False,
	fields=['open', 'high', 'low', 'close', 'settle', 'vol', 'opi', 'pnl', 'logr']):

	print("Loading products data ...")
	products = pd.read_sql("select id, symbol from products", conn)
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

			if frames[fld].empty:
				frames[fld] = df3
			else:
				frames[fld] = pd.merge(frames[fld], df3, how='outer', left_index=True, right_index=True)

	h5 = "{0}_{1}.h5".format(h5, ch)

	if refresh and os.path.exists(h5):
		print("Deleting {0} ...".format(h5))
		os.remove(h5)

	print("Creating file {0} ...".format(h5))
	for fld in frames.keys():
		frames[fld].to_hdf(h5, "daily{0}".format(fld), mode='a')

	return frames



if __name__ == "__main__":
	
	connection = pg.connect(host="localhost", database="futures", user="chenxu", password="")

	for ch in range(1, 10):
		frames = generateHDF(connection, '2000-01-01', ch, 'continuous_chain', True)

