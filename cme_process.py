#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict
import argparse, datetime, quandl

from quandl.errors.quandl_error import NotFoundError

from srf_process import csv2sql

quandl.ApiConfig.api_key = "d5KcMbVrv2GRC2H9Qrn4"
fields = ["open", "high", "low", "close", "change", "settle", "volume", "opi"]

if __name__ == "__main__":

	parser = argparse.ArgumentParser(prog="Quandl CME", description="",
		formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument("--start", nargs="?", type=int, default=1990, dest="start", help="start year")

	args = parser.parse_args()
	
	connection = pg.connect(host="localhost", database="futures", user="chenxu", password="")

	cur = connection.cursor()

	print("Loading table PRODUCTS ...")
	cme_products = pd.read_sql('select id, symbol, month from products where source = \'CME\'', \
		con=connection)
	cme_products.set_index(['id'], inplace=True)

	today = datetime.date.today()
	years = range(args.start, today.year + 2)

	missed = defaultdict(bool)

	frames = []

	for pid, row in cme_products.iterrows():
		months = list(row['month'])
		symbol = row['symbol']

		for year in years:
			for month in months:
				contract = "{0}{1}{2}".format(symbol, month, year)
				expiration = "{0}{1}".format(year, month)

				df = pd.DataFrame()
				try:
					print("Trying product {0} {1} {2}".format(pid, symbol, contract))
					df = quandl.get("CME/{0}".format(contract))
				except NotFoundError:
					print("{0} for product {1} {2} {3}".format(NameError, pid, symbol, contract))
					missed[contract] = True
					continue
				else:
					print("Downloaded {0} {1} {2}".format(pid, symbol, contract))

				if df.empty or not len(df) > 1:
					print("Empty data for product {0} {1} {2}".format(pid, symbol, contract))
					missed[contract] = True
					continue

				df.index.rename("date", inplace=True)
				df.columns = fields

				sql = pd.DataFrame(index=df.index)
				sql.loc[:, "id"]     = pid
				sql.loc[:, "exp"]    = expiration
				sql.loc[:, "chain"]  = 0
				sql.loc[:, "open"]   = df["open"]
				sql.loc[:, "high"]   = df["high"]
				sql.loc[:, "low"]    = df["low"]
				sql.loc[:, "close"]  = df["close"]
				sql.loc[:, "settle"] = df["settle"]
				sql.loc[:, "vol"]    = df["volume"]
				sql.loc[:, "opi"]    = df["opi"]
				sql.loc[:, "pnl"]    = 0
				sql.loc[:, "logr"]   = 0

				sql.reset_index(inplace=True)
				frames.append(sql)

	if len(missed) > 0:
		print("\nMissed symbols:{0}\n".format(','.join(sorted(missed.keys()))))

	""" Create a temporary CSV file for copying to the database """
	results = pd.concat(frames)
	sql_file = "sql_cme.csv"
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

	csv2sql(connection, cur, sql_file, "daily_data", False)

	cur.close()

	print("Committing changes to the database ...")
	connection.commit()
	connection.close()
