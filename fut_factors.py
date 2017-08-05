#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict
from load_h5 import load_continuous_dailydata
import matplotlib.pyplot as plt

def totReturnMomentum(data, N, n):
	return data.rolling(window=N, min_periods=n).sum()

def totReturnMomentum1W(data):
	return totReturnMomentum(data, 5, 4)

def totReturnMomentum2W(data):
	return totReturnMomentum(data, 10, 8)

def totReturnMomentum1M(data):
	return totReturnMomentum(data, 22, 18)

def totReturnMomentum3M(data):
	return totReturnMomentum(data, 66, 57)

def totReturnMomentum6M(data):
	return totReturnMomentum(data, 131, 110)

def totReturnMomentum1Y(data):
	return totReturnMomentum(data, 262, 240)

def totReturnSectorMomentum(products, data, N, n):
	categories = list(products.category.unique())
	sig = totReturnMomentum(data, N, n)

	signals = defaultdict(pd.Series)
	returns = defaultdict(pd.Series)
	for cat in categories:
		cols = list(products[products.category == cat]['symbol'].values)
		sig_temp = sig.copy(deep=True)
		ret_temp = data.copy(deep=True)
		for col in sig_temp.columns:
			if not col in cols:
				sig_temp.drop(col, axis=1, inplace=True)
				ret_temp.drop(col, axis=1, inplace=True)

		signals[cat] = sig_temp.mean(axis=1, skipna=True)
		returns[cat] = ret_temp.mean(axis=1, skipna=True)

	return pd.DataFrame(signals)



if __name__ == "__main__":

	h5 = "continuous_chain_1.h5"
	data = load_continuous_dailydata(h5)
	sig_yr    = totReturnSectorMomentum(data['products'], data['dailylogr'], 262, 250)
	sig_month = totReturnSectorMomentum(data['products'], data['dailylogr'], 22, 18)
	sig_week  = totReturnSectorMomentum(data['products'], data['dailylogr'], 5, 3)
