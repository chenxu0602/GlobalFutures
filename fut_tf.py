#!/usr/local/bin/python3

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import pandas as pd
import sys, os, re
from collections import defaultdict
from load_h5 import load_continuous_dailydata
import matplotlib.pyplot as plt

from fut_factors import *

import itertools

import tensorflow as tf

tf.logging.set_verbosity(tf.logging.INFO)

FEATURES = [
#	'Livestock_Weekly', 'EquityIndex_Weekly', 'Grains_Weekly', 'Metals_Weekly', 'InterestRates_Weekly',
	'Livestock_Daily', 'Livestock_Weekly', 'Livestock_Monthly', 'Livestock_Yearly', 'EquityIndex_Weekly'
]

def input_fn(data):
	feature_cols = {k: tf.constant((data[k].shift(1).fillna(0)).values) for k in FEATURES}
	labels = tf.constant((data['Livestock_Daily'].fillna(0)).values)

	return feature_cols, labels


def main(unused_argv):
	h5 = "continuous_chain_1.h5"
	data = load_continuous_dailydata(h5)
	sig_yr    = totReturnSectorMomentum(data['products'], data['dailylogr'], 262, 250)
	sig_month = totReturnSectorMomentum(data['products'], data['dailylogr'], 22, 18)
	sig_week  = totReturnSectorMomentum(data['products'], data['dailylogr'], 5, 3)
	sig_daily = totReturnSectorMomentum(data['products'], data['dailylogr'], 1, 1)

	feature_dict = {}
	for sym in sig_yr.columns: feature_dict["{0}_Yearly".format(''.join(sym.split()))] = sig_yr[sym]
	for sym in sig_month.columns: feature_dict["{0}_Monthly".format(''.join(sym.split()))] = sig_month[sym]
	for sym in sig_week.columns: feature_dict["{0}_Weekly".format(''.join(sym.split()))] = sig_week[sym]
	for sym in sig_daily.columns: feature_dict["{0}_Daily".format(''.join(sym.split()))] = sig_daily[sym]

	sig = pd.DataFrame(feature_dict)

	train_set = sig[(sig.index >= '2007-01-01') & (sig.index < '2014-01-01')]

	feature_cols = [tf.contrib.layers.real_valued_column(k) for k in FEATURES]

	regressor = tf.contrib.learn.DNNRegressor(feature_columns=feature_cols,
                                            hidden_units=[10, 10],
                                            model_dir="/Users/chenxu/Work/TensorFlow/tmp/futures")

	regressor.fit(input_fn=lambda: input_fn(train_set), steps=5000)

	test_set = sig[(sig.index >= '2014-01-01') & (sig.index < '2017-01-01')]
	ev = regressor.evaluate(input_fn=lambda: input_fn(test_set), steps=1)
	loss_score = ev["loss"]
	print("Loss: {0:f}".format(loss_score))

	y = regressor.predict(input_fn=lambda: input_fn(test_set), as_iterable=False)
  # .predict() returns an iterator; convert to a list and print predictions
#	predictions = list(itertools.islice(y, 6))
#	print("Predictions: {}".format(str(predictions)))

	orig = test_set['EquityIndex_Weekly'].to_frame(name='orig')
	pred = pd.DataFrame({'pred' : y, 'date' : orig.index})
	pred.set_index(['date'], inplace=True)

	df = pd.merge(orig, pred, how='outer', left_index=True, right_index=True)


	print(df.corr())



if __name__ == "__main__":
	tf.app.run()
