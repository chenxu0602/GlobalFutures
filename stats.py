
import os, sys, argparse, re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict
import statsmodels.formula.api as sm

def TSRank(array):
	return pd.Series(array).rank(ascending=False, pct=True).iloc[-1]

class Port:
	def __init__(self, name, sig, ret, w, slip):
		self.name   = name
		self.signal = sig
		self.ret    = ret
		self.weight = w
		self.position = self.signal.fillna(0).shift(1)
		self.tvr = self.position.diff().abs().mean()
		self.nPosition = self.position.count(axis=1)
		self.nLongPosition  = self.position[self.position > 0].count(axis=1)
		self.nShortPosition = self.position[self.position < 0].count(axis=1)

		self.rawPnL = self.ret.mul(self.position.shift(1))
		self.slip   = self.position.diff().abs() * slip / 1e4
		self.PnL    = self.rawPnL - self.slip

		self.wPosition = self.weight.mul(self.position)
		self.wRawPnL   = self.ret.mul(self.wPosition)
		self.wSlip     = self.wPosition.diff().abs() * slip / 1e4
		self.wPnL      = self.wRawPnL - self.wSlip

		self.annualizedRet = self.PnL.mean() * 252.
		self.annualizedStd = self.PnL.std() * 16.
		self.annualizedSharpe = self.annualizedRet.div(self.annualizedStd)

		self.portPosition = self.wPosition.sum(axis=1)
		self.portRawPnL   = self.wRawPnL.sum(axis=1)
		self.portSlip     = self.wSlip.sum(axis=1)
		self.portPnL      = self.wPnL.sum(axis=1)
		self.portTVR      = self.wPosition.diff().abs().sum(axis=1).mean()

		self.portLongPosition  = self.wPosition[self.wPosition > 0].sum(axis=1)
		self.portShortPosition = self.wPosition[self.wPosition < 0].sum(axis=1)

		self.annualizedPortRet  = self.portPnL.mean() * 252.
		self.annualizedPortSlip = self.portSlip.mean() * 252.
		self.annualizedPortStd  = self.portPnL.std() * 16.
		self.annualizedPortSharpe = self.annualizedPortRet / self.annualizedPortStd

	def __str__(self):
		s = "\n\n\n############################################## Strategy: {0} #####################################".format(self.name)
		s += "\nAnnualized Portfolio Return:         {0:.2f}%".format(self.annualizedPortRet * 100)
		s += "\nAnnualized Portfolio Slip:           {0:.2f}%".format(self.annualizedPortSlip * 100)
		s += "\nAnnualized Portfolio Std:            {0:.2f}%".format(self.annualizedPortStd * 100)
		s += "\nAnnualized Portfolio Sharpe ratio:   {0:.2f}".format(self.annualizedPortSharpe)
		s += "\nAverage number of long  positions:   {0:.2f}".format(self.nLongPosition.mean())
		s += "\nAverage number of short positions:   {0:.2f}".format(self.nShortPosition.mean())
		s += "\nAverage long  position:              {0:.2f}%".format(self.portLongPosition.mean() * 100)
		s += "\nAverage short position:              {0:.2f}%".format(self.portShortPosition.mean() * 100)
		s += "\nPortfolio Daily Turnover:            {0:.2f}%".format(self.portTVR * 100)

		return s

class IntraPort(Port):
	def __init__(self, name, sig, ret, w, slip):
		super().__init__(name, sig, ret, w, slip)

		self.dailyRawPnL = self.rawPnL.resample("1D", label="right", closed="right").sum()
		self.dailySlip   = self.slip.resample("1D", label="right", closed="right").sum()
		self.dailyPnL    = self.PnL.resample("1D", label="right", closed="right").sum()
		self.dailyTVR    = self.position.diff().abs().resample("1D", label="right", closed="right").sum()

		self.dailyWRawPnL = self.wRawPnL.resample("1D", label="right", closed="right").sum()
		self.dailyWSlip   = self.wSlip.resample("1D", label="right", closed="right").sum()
		self.dailyWPnL    = self.wPnL.resample("1D", label="right", closed="right").sum()

		self.annualizedRet = self.dailyPnL.mean() * 252.
		self.annualizedStd = self.dailyPnL.std() * 16.
		self.annualizedSharpe = self.annualizedRet.div(self.annualizedStd)

		self.portRawPnL = self.portRawPnL.resample("1D", label="right", closed="right").sum()
		self.portSlip   = self.portSlip.resample("1D", label="right", closed="right").sum()
		self.portPnL    = self.portPnL.resample("1D", label="right", closed="right").sum()
		self.portTVR    = self.wPosition.diff().abs().resample("1D", label="right", closed="right").sum().sum(axis=1).mean()

		self.annualizedPortRet  = self.portPnL.mean() * 252.
		self.annualizedPortSlip = self.portSlip.mean() * 252.
		self.annualizedPortStd  = self.portPnL.std() * 16.
		self.annualizedPortSharpe = self.annualizedPortRet / self.annualizedPortStd

def loadContinuousContracts(datadir, products):
	results = defaultdict(pd.DataFrame)
	for sub in os.listdir(datadir):
		sym, ext = re.split('\.', sub)
		if products and not sub in products: continue
		filename = os.path.join(datadir, sub)
		results[sym] = pd.read_csv(filename, parse_dates=[0], index_col=[0])

	return results

def crack(data, pairs : tuple):
	print("Crack strategy ...")
	leg0 = data[pairs[0]].dropna(axis=0, how="all")
	leg1 = data[pairs[1]].dropna(axis=0, how="all")
	leg2 = data[pairs[2]].dropna(axis=0, how="all")

	start = max(max(leg0.index[0], leg1.index[0]), leg2.index[0])
	leg0 = leg0[leg0.index >= start]
	leg1 = leg1[leg1.index >= start]
	leg2 = leg2[leg2.index >= start]

	settle = pd.DataFrame({pairs[0]:leg0.Settle, pairs[1]:leg1.Settle, pairs[2]:leg2.Settle})
	dsettle = settle.diff()
	dsettle[pairs[0]] = dsettle[pairs[0]] * 42
	dsettle[pairs[1]] = dsettle[pairs[1]] * 42

	ret = pd.DataFrame({pairs[0]:leg0.PnL, pairs[1]:leg1.PnL, pairs[2]:leg2.PnL})

	coef = pd.DataFrame(index=ret.index, columns=[pairs[0], pairs[1]])
	for i in range(252, len(dsettle)):
		temp = dsettle.iloc[i-250:i+1]

		est = sm.ols(formula="{0} ~ {1} + {2} + 0".format(pairs[2], pairs[0], pairs[1]), data = temp).fit()
		print(ret.index[i], est.params[pairs[0]], est.params[pairs[1]])

		coef.loc[dsettle.index[i], pairs[0]] = est.params[pairs[0]]
		coef.loc[dsettle.index[i], pairs[1]] = est.params[pairs[1]]

#    coef = coef.ewm(span=30, min_periods=1, adjust=False).mean()

	settleMV20 = settle.rolling(window=20, min_periods=1).mean()
	settleDebased = settle - settleMV20.fillna(method="bfill").shift(500)

	spread = pd.Series(index=coef.index)
	for idx in spread.index:
		spr = 42 * (\
			settleDebased.loc[idx, pairs[0]] * coef.loc[idx, pairs[0]]\
			+ settleDebased.loc[idx, pairs[1]] * coef.loc[idx, pairs[1]]\
			) - settleDebased.loc[idx, pairs[2]]

		spread[idx] = spr


	sprrank = spread.rolling(window=250, min_periods=1).apply(TSRank)

	sprvol = spread.rolling(window=250, min_periods=1).std()

	signal = pd.DataFrame(index=ret.index, columns=ret.columns)
	for idx in sprrank.index:
		r = sprrank[idx]
#        w = 100 / sprvol[idx]
		w = 1.0
		if r < 0.5:
			signal.loc[idx, pairs[0]] = -0.5 * w
			signal.loc[idx, pairs[1]] = -0.5 * w
			signal.loc[idx, pairs[2]] = 1.0 * w
		elif r > 0.5:
			signal.loc[idx, pairs[0]] = 0.5 * w
			signal.loc[idx, pairs[1]] = 0.5 * w
			signal.loc[idx, pairs[2]] = -1.0 * w
		else:
			signal.loc[idx, pairs[0]] = 0
			signal.loc[idx, pairs[1]] = 0
			signal.loc[idx, pairs[2]] = 0

	return signal, ret



if __name__ == "__main__":
	parser = argparse.ArgumentParser(prog="Load Continuous Futures Contracts", description="",
		formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument("--rootdir", nargs="?", type=str, default="/Users/chenxu/Work/GlobalFutures", 
		dest="rootdir", help="root directory")
	parser.add_argument("--datadir", nargs="?", type=str, default="continuous", dest="datadir", help="continuous data dir")
	parser.add_argument("--outdir", nargs="?", type=str, default="continuous", dest="outdir", help="output dir")
	parser.add_argument("--products", nargs="*", type=str, default=[], dest="products", help="product list")

	args = parser.parse_args()

	datadir = os.path.join(args.rootdir, args.datadir)
	if not os.path.exists(datadir):
		print("Continuous data directory {0} doesn't exist!".format(datadir))
		sys.exit(1)

	continuous = loadContinuousContracts(datadir, args.products)

	signals, returns = crack(continuous, ('O', 'N', 'B'))
	weights = pd.DataFrame(index=signals.index, columns=signals.columns)
	weights.fillna(1, inplace=True)

	st = Port("Crack", signals, returns, weights, 0)

	"""
	fig, ax = plt.subplots()
	pd.DataFrame({"Cum PnL (BTC-LTC)": intraStats2.portPnL.cumsum()}).plot(ax=ax, grid=True)
	plt.legend(loc="best")

	import matplotlib.dates as mdates
	ax.xaxis.set_major_locator(mdates.WeekdayLocator())
	ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
	"""

	"""
	pd.DataFrame({"Cum PnL (3-Day Reversion)": stats.portPnL.cumsum()}).plot(grid=True)
	plt.legend(loc="best")
	"""
