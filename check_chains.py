#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict

from datetime import datetime

def checkChain(conn, start):
	print("Loading daily data ...")
	dailydata = pd.read_sql("select date, id, chain, exp from daily_data where date >= \'{0}\'".format(\
		start), conn)

	print("Resetting index ...")
	dailydata.set_index(['id', 'date', 'chain'], inplace=True)
	print("Sorting index ...")
	dailydata.sort_index(level=['id', 'date', 'chain'], inplace=True)

	for pid, df in dailydata.groupby(level='id'):
		df2 = df.xs(pid, level='id')
		for dt, df3 in df2.groupby(level='date'):
			df4 = df3.xs(dt, level='date')
			indices = list(df4.index.unique())
			for ix in indices:
				if ix > 0:
					n = len(df4.loc[ix])
					if not n == 1:
						expirations = list(df4.loc[ix, 'exp'].values)
						print("{0} {1} has {2} chain {3}: {4}".format(pid, dt.strftime("%Y-%m-%d"), n, ix, expirations))


if __name__ == "__main__":

	connection = pg.connect(host="localhost", database="futures", user="chenxu", password="")
	checkChain(connection, '2000-01-01')

