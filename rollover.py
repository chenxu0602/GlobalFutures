#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict
from datetime import date, datetime, timedelta
import operator

from srf_process import csv2sql

mon2char = {
	'F' : 1,
	'G' : 2,
	'H' : 3,
	'J' : 4,
	'K' : 5,
	'M' : 6,
	'N' : 7,
	'Q' : 8,
	'U' : 9,
	'V' : 10,
	'X' : 11,
	'Z' : 12
}

def mode(l):
	if not len(l) > 0:
		print("None mode because of zero values!")
		return None 

	freq = defaultdict(int)
	for i in l:
		freq[i] += 1

	sorted_freq = sorted(freq.items(), key=operator.itemgetter(1), reverse=True)
	if not len(sorted_freq) > 0:
		return None

	frq = sorted_freq[0][1]
	mean = sorted_freq[0][0]
	count = 1

	if len(sorted_freq) > 1:
		for i in range(1, len(sorted_freq)):
			if not frq == sorted_freq[i][1]:
				break
			else:
				count += 1
				mean += sorted_freq[0][0]
				if frq == 1 and count == 3:
					break

	return (mean / count)


def dateRange(dt1, dt2):
	for i in range(0, int((dt2-dt1).days)+1):
		yield dt1 + timedelta(i)

def dateRangeReverse(dt1, dt2):
	for i in range(0, int((dt2-dt1).days)+1):
		yield dt2 - timedelta(i)

def roll(conn=None, field="vol", last="2016Z"):
	print("Loading daily data ...")
	data = pd.read_sql("select date, id, exp, vol, opi from daily_data", conn)
	data.set_index(['id', 'exp', 'date'], inplace=True)

	print("Getting the first and last tradeable dates ...")
	start = defaultdict(lambda: defaultdict(datetime.date))
	end   = defaultdict(lambda: defaultdict(datetime.date))

	for pid, df in data.groupby(level="id"):
		df2 = df.xs(pid, level="id")
		for exp, df3 in df2.groupby(level="exp"):
			df4 = df3.xs(exp, level="exp")
			start[pid][exp] = df4.index[0]
			if not exp > last:
				end[pid][exp] = df4.index[-1]
			else:
				year = int(exp[:4])
				mon  = mon2char[exp[4]]
				mon += 1
				if mon == 13:
					mon = 1 
					year += 1

				end[pid][exp] = datetime(year, mon, 1) - timedelta(days=2)


	print("Getting the optimal rollover ...")
	optimal = defaultdict(lambda: defaultdict(int))

	for pid in sorted(end.keys()):
		contracts = sorted(end[pid].keys())

		n_contracts = len(contracts)
		if not n_contracts > 1:
			print("Product {0} doesn't have enough contracts!".format(pid))
			continue

		for i in range(0, n_contracts - 1):
			con0    = contracts[i]
			data0   = data.xs(pid, level="id").xs(con0, level="exp")
			start0  = start[pid][con0]
			end0    = end[pid][con0]

			optRoll = None
			for j in range(i+1, min(i+3, n_contracts)):
				con1 = contracts[j]

				data1   = data.xs(pid, level="id").xs(con1, level="exp")

				""" If there is no choice, roll on the last tradeable date """
				start1 = start[pid][con1]
				end1   = end[pid][con1]

				if end0 <= start1:
					optimal[pid][exp] = 0
				else:
					for dt in dateRangeReverse(start1, end0):
						if dt in data0.index and dt in data1.index:
							val0 = data0.loc[dt, field]
							val1 = data1.loc[dt, field]
							if val0 > val1:
								optRoll = dt
								break

			if not optRoll:
				optRoll = end0

#			print("Optimal roll for {0} {1} is {2}. {3}".format(pid, con0, optRoll.strftime("%Y-%m-%d"), end0))
			optimal[pid][con0] = (end0 - optRoll).days

	print("Calculating the prediction ...")
	prediction = defaultdict(lambda: defaultdict(datetime.date))
	for pid in sorted(end.keys()):
		contracts = sorted(end[pid].keys())
		n_contracts = len(contracts)
		if not n_contracts > 12:
			print("Product {0} doesn't have enough contracts for prediction!".format(pid))
			continue

		for i in range(12, n_contracts):
			values = []
			con0 = contracts[i]
			for j in range(i-12, i):
				con1 = contracts[j]
				if con1 <= last: 
					ndays = optimal[pid][con1]
					values.append(ndays)

			lasttradeable = end[pid][con0]
			m = mode(values)
			if m == None:
				print("Couldn't find mode for product {0} contract {1}.".format(pid, con0))
			else:
				prediction[pid][con0] = lasttradeable - timedelta(days=m)

	cur = conn.cursor()

	sql_contracts = pd.read_sql('select id, exp, start, last, roll from contracts', con=conn)
	sql_contracts.set_index(["id", "exp"], inplace=True)
	sql_contracts.drop(sql_contracts.index, inplace=True)

	for pid in sorted(prediction.keys()):
		for exp in sorted(prediction[pid].keys()):
			s = start[pid][exp].strftime("%Y-%m-%d")
			e = end[pid][exp].strftime("%Y-%m-%d")
			o = optimal[pid][exp]
			p = prediction[pid][exp].strftime("%Y-%m-%d")

			print("Contract {0} {1} start {2} end {3} optimal {4} prediction {5}.".format(pid, exp, s, e, o, p))
			sql_contracts.loc[(pid, exp), sql_contracts.columns] = [s, e, p]

	sql_contracts.reset_index(inplace=True)
	sql_file = "contracts.csv"
	print("Saving contracts to CSV file {0} ...".format(sql_file))
	sql_contracts.to_csv(sql_file, sep='\t', header=False, na_rep='\\N', index=False, \
		columns=['id', 'exp', 'start', 'last', 'roll'])

	csv2sql(conn, cur, sql_file, "contracts")

	cur.close()

	print("Committing changes to the database ...")
	conn.commit()

	return prediction


def chain(conn, sql_begin):
	print("Loading the products data ...")
	products = pd.read_sql("select id, symbol, month from products", conn)
	products.set_index(['id'], inplace=True)

	print("Loading the contracts data ...")
	rolldates = pd.read_sql("select id, exp, start, last, roll from contracts", con=conn)
	rolldates.set_index(['id', 'exp'], inplace=True)
	rolldates.sort_index(level=['id', 'exp'], inplace=True)

	print("Loading the daily data ...")
	dailydata = pd.read_sql(\
		"select date,id,exp,open,high,low,close,settle,vol,opi,pnl,logr from daily_data where date >= \'{0}\'".format(\
		sql_begin), conn)
	print("Resetting index ...")
	dailydata.set_index(['id', 'date', 'exp'], inplace=True)
	print("Sorting index ...")
	dailydata.sort_index(level=['id', 'date', 'exp'], inplace=True)

	cur = conn.cursor()

	for pid, df in dailydata.groupby(level='id'):
		df2 = df.xs(pid, level='id')
		df_contracts = rolldates.xs(pid, level='id')
		months = products.loc[pid, 'month']

		for dt, df3 in df2.groupby(level='date'):
			df4 = df3.xs(dt, level='date')
			contracts = df4.index

			chain = 0
			for i in range(0, len(contracts)):
				con = contracts[i]

				if not con in df_contracts.index:
					print("{0} {1} is not among the available contracts".format(pid, con))
					continue

				if not con[-1] in list(months):
					print("{0} {1} is not among the good months {2}".format(pid, con, months))
					continue

				start = df_contracts.loc[con, 'start']
				roll  = df_contracts.loc[con, 'roll']

				if dt.date() >= start and dt.date() <= roll:
					chain += 1
					dailydata.loc[(pid, dt, con), 'chain'] = chain
					print("Product {0}  {1}    {2}   chain: {3}".format(pid, dt.strftime("%Y-%m-%d"), con, chain))	

	print("Setting chain value NaN to 0 ...")
	dailydata['chain'].fillna(0, inplace=True)
	print("Converting daily data chain to integer ...")
	dailydata['chain'] = dailydata['chain'].astype(int)

	sql_file = "daily_data.csv"
	print("Saving daily data to CSV file {0} ...".format(sql_file))
	if os.path.exists(sql_file):
		os.remove(sql_file)

	dailydata.reset_index(inplace=True)
	dailydata.to_csv(sql_file, sep="\t", header=False, na_rep="\\N", index=False,\
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

	print("Committing changes to the database ...")
	conn.commit()

	return dailydata

if __name__ == "__main__":
	
	connection = pg.connect(host="localhost", database="futures", user="chenxu", password="")
	
	print("Calculating the rollover dates ...")
	data = roll(connection)

	print("Set the chain numbers ...")
	data = chain(connection, '1997-01-01')

	connection.close()
