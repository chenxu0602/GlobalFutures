#!/usr/local/bin/python3

import pandas as pd
import psycopg2 as pg


def upload(conn=None, srf_path="/Users/chenxu/Work/GlobalFutures/SRF_20170111.csv", \
	fields=["symbol", "date", "open", "high", "low", "settle", "volume", "opi"]):
	print("Loading SRF data file {0} with fields {1} ...".format(srf_path, ','.join(fields)))
#	srf = pd.read_csv(srf_path, parse_dates=[1], header=None, names=fields)

	cur = conn.cursor()
	cur.execute("select * from srf_products_list")
	rows = cur.fetchall()
	
	sym2Id = dict()
	for row in rows:
		if not len(row) >= 6: 
			continue
		pid = row[0]
		sym = row[1]
		sym2Id[sym] = pid

	cur.close()

	print(sym2Id)



if __name__ == "__main__":
	
	connection = pg.connect(host="localhost", database="futures", user="chenxu", password="postgres")
	upload(connection)

	connection.close()

