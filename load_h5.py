#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict
from datetime import datetime, date

def load_WikiEOD(h5="WikiEOD.h5", start=datetime(2000, 1, 1)):
	print("Loading Wiki EOD daily data from {0} ...".format(h5))
	dailyopen       = pd.read_hdf(h5, "open")
	dailyhigh       = pd.read_hdf(h5, "high")
	dailylow        = pd.read_hdf(h5, "low")
	dailyclose      = pd.read_hdf(h5, "close")
	adjopen         = pd.read_hdf(h5, "adjopen")
	adjhigh         = pd.read_hdf(h5, "adjhigh")
	adjlow          = pd.read_hdf(h5, "adjlow")
	adjclose        = pd.read_hdf(h5, "adjclose")
	div             = pd.read_hdf(h5, "div")
	split           = pd.read_hdf(h5, "split")
	volume          = pd.read_hdf(h5, "volume")
	adjvol          = pd.read_hdf(h5, "adjvol")

	dailyopen  = dailyopen.loc[dailyopen.index >= start]
	dailyhigh  = dailyhigh.loc[dailyhigh.index >= start]
	dailylow   = dailylow.loc[dailylow.index >= start]
	dailyclose = dailyclose.loc[dailyclose.index >= start]
	adjopen    = adjopen.loc[adjopen.index >= start]
	adjhigh    = adjhigh.loc[adjhigh.index >= start]
	adjlow     = adjlow.loc[adjlow.index >= start]
	adjclose   = adjclose.loc[adjclose.index >= start]
	div   	  = div.loc[div.index >= start]
	split      = split.loc[split.index >= start]
	volume     = volume.loc[volume.index >= start]
	adjvol     = adjvol.loc[adjvol.index >= start]

	data = defaultdict(pd.DataFrame)
	data["open"]    	 = dailyopen
	data["high"]       = dailyhigh
	data["low"]        = dailylow
	data["close"]      = dailyclose
	data["adjopen"]    = adjopen
	data["adjhigh"]    = adjhigh
	data["adjlow"]     = adjlow
	data["adjclose"]   = adjclose
	data["div"]        = div
	data["split"]      = split
	data["volume"]     = volume
	data["adjvol"]     = adjvol

	return data


def load_continuous_dailydata(h5="continuous_chain_1.h5"):
	print("Loading continuous daily data from {0} ...".format(h5))
	products    = pd.read_hdf(h5, "products")
	dailyopen   = pd.read_hdf(h5, "dailyopen")
	dailyhigh   = pd.read_hdf(h5, "dailyhigh")
	dailylow    = pd.read_hdf(h5, "dailylow")
	dailyclose  = pd.read_hdf(h5, "dailyclose")
	dailysettle = pd.read_hdf(h5, "dailysettle")
	dailyvol    = pd.read_hdf(h5, "dailyvol")
	dailypnl    = pd.read_hdf(h5, "dailypnl")
	dailylogr   = pd.read_hdf(h5, "dailylogr")

	data = defaultdict(pd.DataFrame)
	data["products"]      = products
	data["dailyopen"]     = dailyopen
	data["dailyhigh"]     = dailyhigh
	data["dailylow"]      = dailylow
	data["dailyclose"]    = dailyclose
	data["dailysettle"]   = dailysettle
	data["dailyvol"]      = dailyvol
	data["dailypnl"]      = dailypnl
	data["dailylogr"]     = dailylogr

	return data



if __name__ == "__main__":

	h5 = "continuous_chain_1.h5"
	data = load_continuous_dailydata(h5)
	

