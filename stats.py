#!/usr/local/bin/python3

import numpy as np
import pandas as pd
import psycopg2 as pg
import sys, os, re
from collections import defaultdict
from load_h5 import load_continuous_dailydata
import matplotlib.pyplot as plt


if __name__ == "__main__":

	fut_h5 = "continuous_chain_1.h5"
	eq_h5  = "WikiEOD.h5"

	fut_data = load_continuous_dailydata(fut_h5)
	eq_data  = load_continuous_dailydata(eq_h5)
