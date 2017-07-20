#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict

from srf_process import csv2sql

def cleanTS(ts=pd.DataFrame, threshold=0.5):
	prev_ts = ts.shift(1)
	post_ts = ts.shift(-1)

	log_prev_ts = np.log(prev_ts)
	log_ts      = np.log(ts)
	log_post_ts = np.log(post_ts)

	ts[(log_ts - log_prev_ts > threshold)  & (log_ts - log_post_ts > threshold)]  = np.nan
	ts[(log_ts - log_prev_ts < -threshold) & (log_ts - log_post_ts < -threshold)] = np.nan

	ts.fillna(method='ffill', inplace=True)


def updateDailyData(conn=None, \
	fields=[
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
		"logr" ]):

	print("Preparing to update the daily data with fields {0} ...".format(','.join(fields)))
	data = pd.read_sql("select {0} from daily_data".format(','.join(fields)), con=conn)
	data.set_index(['id', 'exp', 'date'], inplace=True)

	cur = conn.cursor()

	frames = []

	for pid, df in data.groupby(level="id"):
		df2 = df.xs(pid, level="id")
		for exp, df3 in df2.groupby(level="exp"):
			df4 = df3.xs(exp, level="exp")

			print("Cleaning data for {0} {1} ...".format(pid, exp))
			op    = df4["open"].copy(deep=True)
			hi    = df4["high"].copy(deep=True)
			lo    = df4["low"].copy(deep=True)
			cl    = df4["close"].copy(deep=True)
			st    = df4["settle"].copy(deep=True)
			vol   = df4["vol"].copy(deep=True)
			opi   = df4["opi"].copy(deep=True)

			""" Set negative price to NaN """
			op[~(op > 0)]   = np.nan
			hi[~(hi > 0)]   = np.nan
			lo[~(lo > 0)]   = np.nan
			cl[~(cl > 0)]   = np.nan
			st[~(st > 0)]   = np.nan
			vol[~(vol > 0)] = np.nan
			opi[~(opi > 0)] = np.nan

			""" Set outliers to their previous values """

			""" Forward fill NaNs and """
			op.fillna(method='ffill', inplace=True)
			hi.fillna(method='ffill', inplace=True)
			lo.fillna(method='ffill', inplace=True)
			cl.fillna(method='ffill', inplace=True)
			st.fillna(method='ffill', inplace=True)
			vol.fillna(0, inplace=True)
			opi.fillna(0, inplace=True)

			""" Remove outliers """
			cleanTS(op)
			cleanTS(hi)
			cleanTS(lo)
			cleanTS(cl)
			cleanTS(st)

			pnl  = st.pct_change(periods=1)
			logr = np.log(st).diff(periods=1)

			""" dataframe sql has "date" as index """
			sql = pd.DataFrame(index=df4.index)
			sql.loc[:, "id"]     = pid
			sql.loc[:, "exp"]    = exp
			sql.loc[:, "chain"]  = 0
			sql.loc[:, "open"]   = op
			sql.loc[:, "high"]   = hi
			sql.loc[:, "low"]    = lo
			sql.loc[:, "close"]  = cl
			sql.loc[:, "settle"] = st
			sql.loc[:, "vol"]    = vol
			sql.loc[:, "opi"]    = opi
			sql.loc[:, "pnl"]    = pnl
			sql.loc[:, "logr"]   = logr

			sql.reset_index(inplace=True)
			frames.append(sql)

	results = pd.concat(frames)
	sql_file = "sql_clean.csv"
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

	csv2sql(conn, cur, sql_file, "daily_data")

	cur.close()

	return results

if __name__ == "__main__":
	
	connection = pg.connect(host="localhost", database="futures", user="chenxu", password="")
	data = updateDailyData(connection)

	print("Committing changes to the database ...")
	connection.commit()
	connection.close()
