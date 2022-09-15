from pickletools import read_uint1
from random import sample
from pyJoules.energy_meter import measure_energy
from pyJoules.handler.csv_handler import CSVHandler

csv_handler = CSVHandler('trial_st_10.csv')
import time
import pandas as pd


# I/O functions - READ
@measure_energy(handler=csv_handler)
def load_csv(path):
    return pd.read_csv(path)


@measure_energy(handler=csv_handler)
def load_hdf(path):
    return pd.read_hdf(path)


@measure_energy(handler=csv_handler)
def load_json(path):
    return pd.read_json(path)


# I/O functions - WRITE
@measure_energy(handler=csv_handler)
def save_csv(df, path):
    return df.to_csv(path)


@measure_energy(handler=csv_handler)
def save_hdf(df, path, key):
    return df.to_hdf(path, key=key)


@measure_energy(handler=csv_handler)
def save_json(df, path):
    return df.to_json(path)


###------------------------------------------###

# Handling missing data
@measure_energy(handler=csv_handler)
def isna(df, cname):
    return df[cname].isna()


@measure_energy(handler=csv_handler)
def dropna(df):
    return df.dropna()


@measure_energy(handler=csv_handler)
def fillna(df, val):
    return df.fillna(val)


@measure_energy(handler=csv_handler)
def replace(df, cname, src, dest):
    return df[cname].replace(src, dest)


# Table operations
# drop column
# groupby
# merge
# transpose
# sort
# concat
@measure_energy(handler=csv_handler)
def drop(df, cnameArray):
    return df.drop(columns=cnameArray)


@measure_energy(handler=csv_handler)
def groupby(df, cname):
    return df.groupby(cname)


@measure_energy(handler=csv_handler)
def merge(df1, df2, on=None):
    if (on):
        return pd.merge(df1, df2, on=on)
    else:
        return pd.merge(df1, df2)


@measure_energy(handler=csv_handler)
def sort(df, cname):
    return df.sort_values(by=[cname])


# def transpose(df):
#     return df.transpose()

@measure_energy(handler=csv_handler)
def concat_dataframes(df1, df2):
    return pd.concat([df1, df2])


# Statistical Operations
# min, max, mean, count, unique, correlation

# count
@measure_energy(handler=csv_handler)
def count(df):
    return df.count()


# sum
@measure_energy(handler=csv_handler)
def sum(df, cname):
    return df[cname].sum()


# mean
@measure_energy(handler=csv_handler)
def mean(df):
    return df.mean()


# min
@measure_energy(handler=csv_handler)
def min(df):
    return df.min()


# max
@measure_energy(handler=csv_handler)
def max(df):
    return df.max()


# unique
@measure_energy(handler=csv_handler)
def unique(df):
    return df.unique()
