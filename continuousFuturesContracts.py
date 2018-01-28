#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import sys, os, re
from collections import defaultdict 
import argparse, datetime, quandl 
import matplotlib.pyplot as plt
import datetime

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

def loadRawData(rawdir, dirs, products):
    results = defaultdict(lambda: defaultdict(pd.DataFrame))

    for sub in dirs:
        if products and not sub in products:
            continue

        productdir = os.path.join(rawdir, sub)
        print("Loading data from {0} ...".format(productdir))

        for con in os.listdir(productdir):
            contract, ext = re.split('\.', con)
            filename = os.path.join(productdir, con)
            df = pd.read_csv(filename, parse_dates=[0], index_col=[0])
            df["Settle"].fillna(df["Close"], inplace=True)
            df["PnL"] = np.log(df["Close"]).diff()
            results[sub][contract] = df

    return results

def chain(cons, fld="Volume"):
    results = defaultdict(lambda: pd.DataFrame)
    for sym in sorted(cons.keys()):
        ch = signalChain(sym, cons[sym], fld)
        results[sym] = ch

    return results

def signalChain(sym, cons, fld):
    results = defaultdict(lambda: pd.Series)
    for contract in sorted(cons.keys()):
        results[contract] = cons[contract][fld]

    df = pd.DataFrame(results)
    df_mv5 = df.rolling(window=5, min_periods=1).mean()
    df_mv5_rank = df_mv5.rank(axis=1, pct=False, ascending=False)

    res = pd.DataFrame(index=df.index)

    if not df_mv5_rank.empty:
        for i in range(0, len(df_mv5_rank)):
            date = df_mv5_rank.index[i]
            candidates = df_mv5_rank.columns[(df_mv5_rank == 1).iloc[i]]
            if len(candidates) > 0:
                symbol = candidates[0]
                data   = cons[symbol]
                res.loc[date, "Open"]   = data.loc[date, "Open"]
                res.loc[date, "High"]   = data.loc[date, "High"]
                res.loc[date, "Low"]    = data.loc[date, "Low"]
                res.loc[date, "Close"]  = data.loc[date, "Close"]
                res.loc[date, "Settle"] = data.loc[date, "Settle"]
                res.loc[date, "Volume"] = data.loc[date, "Volume"]
                res.loc[date, "OI"]     = data.loc[date, "OI"]
                res.loc[date, "PnL"]    = data.loc[date, "PnL"]
            else:
                print("{0} doesn't have any contracts on date {1}!".format(sym, date))
                continue
    else:
        print("{0} doesn't have data!".format(sym))

    return res


if __name__ == "__main__":

    parser = argparse.ArgumentParser(prog="Generate Continuous Futures Contracts", description="",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--rootdir", nargs="?", type=str, default="/Users/chenxu/Work/GlobalFutures", 
        dest="rootdir", help="root directory")
    parser.add_argument("--rawdatadir", nargs="?", type=str, default="rawdata.2018-01-29", dest="rawdatadir", help="raw data dir")
    parser.add_argument("--outputdir", nargs="?", type=str, default="continous", dest="outputdir", help="output dir")
    parser.add_argument("--products", nargs="*", type=str, default=[], dest="products", help="product list")
    parser.add_argument("--fields", nargs="*", type=str, default=["Open", "High", "Low", "Close", "Settle", "Volume", "OI"], dest="fields", help="fields")

    args = parser.parse_args()

    rawdatadir = os.path.join(args.rootdir, args.rawdatadir)
    if not os.path.exists(rawdatadir):
        print("Raw data {0} doesn't exist!".format(rawdatadir))
        sys.exit(1)

    subdirs = os.listdir(rawdatadir)
    contracts = loadRawData(rawdatadir, subdirs, args.products)

    continuous = chain(contracts, "Volume")