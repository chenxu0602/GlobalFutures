#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict
from load_h5 import load_continuous_dailydata, load_WikiEOD
import matplotlib.pyplot as plt
from datetime import datetime, date
from fut_factors import *

def getVol(data):
	logr = data["logr"]
	sd   = logr.rolling(window=250, min_periods=200).std()
	data["std"] = sd

	return data

def getATR(data):
	print("Getting ATR ...")
	high  = data["adjhigh"]
	low   = data["adjlow"]
	close = data["adjclose"]

	hl = high - low
	hc = (high - close.shift()).abs()
	lc = (low  - close.shift()).abs()

	df_atr = pd.DataFrame(index=close.index)

	for col in hl.columns:
		print("Calculating ATR for {0} ...".format(col))
		df = pd.DataFrame({"HL":hl[col], "HC":hc[col], "LC":lc[col]})
		df_max = df.max(axis=1).to_frame(name=col)
		df_atr = pd.merge(df_atr, df_max, how='outer', left_index=True, right_index=True)

	data["tr"]   = df_atr
	data["atr"]  = df_atr.ewm(com=14, min_periods=1).mean()
	data["atr2"] = data["atr"].div(close.rolling(window=5, min_periods=3).mean())

	return data

def getReturns(data):
	print("Getting the returns ...")
	adjclose   = data["adjclose"]
	data["pnl"] = adjclose.pct_change()
	data["logr"] = np.log(adjclose).diff()

	return data

def getMktCapRank(data):
	print("Getting the liquid universe ...")
	dailyopen  = data["open"]
	dailyhigh  = data["high"]
	dailylow   = data["low"]
	dailyclose = data["close"]
	adjopen    = data["adjopen"]
	adjhigh    = data["adjhigh"]
	adjlow     = data["adjlow"]
	adjclose   = data["adjclose"]
	div        = data["div"]
	split      = data["split"]
	volume     = data["volume"]
	adjvol     = data["adjvol"]

	mktcap     = volume * dailyclose
	mktcap20   = mktcap.rolling(window=20, min_periods=10).mean()

	data["mktcap"] = mktcap20
	data["mktcaprank"] = mktcap20.rank(axis=1, na_option='keep', ascending=False)
	data["volumerank"] = adjvol.rank(axis=1, na_option='keep', ascending=False)

	return data

def save(data, h5):
	print("Saving data to {0} ...".format(h5))

	if os.path.exists(h5): os.remove(h5)
	for key in sorted(data.keys()):
		df = data[key]
		df.to_hdf(h5, key, model="a")


if __name__ == "__main__":

	start = datetime(2000, 1, 1)

	h5 = "WikiEOD.h5"
	data = load_WikiEOD(h5, start)
	data = getMktCapRank(data)
	data = getReturns(data)
	data = getVol(data)
	data = getATR(data)

	h5_new = "WikiEOD_Params.h5"
	save(data, h5_new)

	sig1Y = totReturnMomentum1Y(data["logr"])
	sig1M = totReturnMomentum1M(data["logr"])

	sig1YRank = sig1Y.rank(axis=1, pct=True, na_option='keep', ascending=False)
	sig1MRank = sig1M.rank(axis=1, pct=True, na_option='keep', ascending=False)
