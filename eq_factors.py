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
from eq_params import *

def lowVol(h5):
	sd   = pd.read_hdf(h5, "std")
	atr  = pd.read_hdf(h5, "atr2")
	logr = pd.read_hdf(h5, "logr")
	mktcaprank = pd.read_hdf(h5, "mktcaprank")
	sdRank  = sd.rank(axis=1, pct=True, na_option='keep', ascending=True)
	atrRank = atr.rank(axis=1, pct=True, na_option='keep', ascending=True)

	signal = pd.DataFrame(index=sdRank.index, columns=sdRank.columns)
	signal[(mktcaprank < 1000) & (sdRank < 0.05) & (atrRank < 0.05)] = 1.0

	return signal


def equityMom1Y(h5):
	mktcaprank = pd.read_hdf(h5, "mktcaprank")
	sd   = pd.read_hdf(h5, "std")
	atr  = pd.read_hdf(h5, "atr2")
	logr = pd.read_hdf(h5, "logr")
	sig1Y = totReturnMomentum1Y(logr).shift(20)
	sig6M = totReturnMomentum6M(logr)
	sig1M = totReturnMomentum1M(logr)
	sig1Y_vol = sig1Y.div(atr)
	sig6M_vol = sig6M.div(atr)
	sig1M_vol = sig1M.div(atr)
	div = pd.read_hdf(h5, "div")
	adjvol = pd.read_hdf(h5, "adjvol")
	close = pd.read_hdf(h5, "close")
	divYld = div.div(close).rolling(window=250, min_periods=200).mean()
	divRank = divYld.rank(axis=1, pct=True, na_option='keep', ascending=False)
	volRank = adjvol.rank(axis=1, pct=False, na_option='keep', ascending=False)

	signal1 = pd.DataFrame(index=sig1Y.index, columns=sig1Y.columns)
	rank = sig1Y.rank(axis=1, pct=True, na_option='keep', ascending=False)
	signal1[(rank < 0.3)] = 1.0

	signal2 = pd.DataFrame(index=sig1Y_vol.index, columns=sig1Y_vol.columns)
	rank = sig1Y_vol.rank(axis=1, pct=True, na_option='keep', ascending=False)
	signal2[(rank < 0.3)] = 1.0

	signal3 = pd.DataFrame(index=sig6M.index, columns=sig6M.columns)
	rank = sig6M.rank(axis=1, pct=True, na_option='keep', ascending=False)
	signal3[(rank < 0.3)] = 1.0

	signal4 = pd.DataFrame(index=sig6M_vol.index, columns=sig6M_vol.columns)
	rank = sig6M_vol.rank(axis=1, pct=True, na_option='keep', ascending=False)
	signal4[(rank < 0.3)] = 1.0

	signal5 = pd.DataFrame(index=sig1M.index, columns=sig1M.columns)
	rank = sig1M.rank(axis=1, pct=True, na_option='keep', ascending=False)
	signal5[(rank > 0.4)] = 1.0

	signal6 = pd.DataFrame(index=sig1M_vol.index, columns=sig1M_vol.columns)
	rank = sig1M_vol.rank(axis=1, pct=True, na_option='keep', ascending=False)
	signal6[(rank > 0.4)] = 1.0

	signal7 = pd.DataFrame(index=atr.index, columns=atr.columns)
	rank = atr.rank(axis=1, pct=True, na_option='keep', ascending=False)
	signal7[(rank > 0.3)] = 1.0

	sdRank  = sd.rank(axis=1, pct=True, na_option='keep', ascending=True)
	atrRank = atr.rank(axis=1, pct=True, na_option='keep', ascending=True)

	signal = pd.DataFrame(index=mktcaprank.index, columns=mktcaprank.columns)
	signal[(mktcaprank < 1000) & (signal1 > 0) & (signal2 > 0) & (signal3 > 0) \
		& (signal4 > 0) & (sdRank < 0.1) & (atrRank < 0.1)] = 1.0

	signal.fillna(0, inplace=True)

	return signal


if __name__ == "__main__":

	h5 = "WikiEOD_Params.h5"
	sig = equityMom1Y(h5)
#	sig = lowVol(h5)
	ret = pd.read_hdf(h5, "pnl")
	pnl = sig.shift() * ret
	totpnl = pnl.sum(axis=1)
	totpnl.cumsum().plot()
