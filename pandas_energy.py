import time
from pickletools import read_uint1
from random import sample

import pandas as pd
from pyJoules.energy_meter import measure_energy
from pyJoules.handler.csv_handler import CSVHandler

df = pd.DataFrame()
df_samp = pd.DataFrame()

# I/O functions - READ


def load_csv(path):
    return pd.read_csv(path)


def load_hdf(path):
    return pd.read_hdf(path)


def load_json(path):
    return pd.read_json(path)


# I/O functions - WRITE
def save_csv(df, path):
    return df.to_csv(path)


def save_hdf(df, path, key):
    return df.to_hdf(path, key=key)


def save_json(df, path):
    return df.to_json(path)


###------------------------------------------###

# Handling missing data
def isna(df, cname):
    return df[cname].isna()


def dropna(df):
    return df.dropna()


def fillna(df, val):
    return df.fillna(val)


def replace(df, cname, src, dest):
    return df[cname].replace(src, dest)


###------------------------------------------###

# Table operations
# drop column
# groupby
# merge
# transpose
# sort
# concat
def drop(df, cnameArray):
    return df.drop(columns=cnameArray)


def groupby(df, cname):
    return df.groupby(cname)


def merge(df1, df2, on=None):
    if on:
        return pd.merge(df1, df2, on=on)
    else:
        return pd.merge(df1, df2)


def sort(df, cname):
    return df.sort_values(by=[cname])


# def transpose(df):
#     return df.transpose()


def concat_dataframes(df1, df2):
    return pd.concat([df1, df2])


###--------------------------------------------###
# Statistical Operations
# min, max, mean, count, unique, correlation

# count
def count(df):
    return df.count()


# sum
def sum(df, cname):
    return df[cname].sum()


# mean
def mean(df):
    return df.mean()


# min
def min(df):
    return df.min()


# max
def max(df):
    return df.max()


# unique
def unique(df):
    return df.unique()


# @measure_energy(handler=csv_handler)
# def drop_column(df, col_names=[]):
#     df.drop(columns=col_names)

# @measure_energy(handler=csv_handler)
# def remove_duplicates(df):
#     return df.drop_duplicates()

# @measure_energy(handler=csv_handler)
# def merge(df1, df2, on=None):
#     if(on):
#         return pd.merge(df1, df2, on=on)
#     else:
#         return pd.merge(df1, df2)

# @measure_energy(handler=csv_handler)
# def group_by(df, groupby):
#     pass

# # subset
# @measure_energy(handler=csv_handler)
# def subset(df, subset):
#     return df[subset]

# # sample
# @measure_energy(handler=csv_handler)
# def sample(df, cnt=1000):
#     return df.sample(cnt)

# count, mean, min, max, value_counts, unique, sort values, groupby

# Input output functions
def reset_env():
    global df, df_samp
    df = pd.read_csv("Datasets/drugs.csv")
    df_samp = pd.read_csv("Datasets/drugs.csv")


def test_load_csv():
    df = load_csv(path="Datasets/drugs.csv")


def test_load_json():
    df = load_json(path="Datasets/drugs.json")


def test_load_hdf():
    df = load_hdf(path="Datasets/drugs.h5")


def test_save_csv():
    save_csv(df, "Datasets/drugs.csv")


def test_save_json():
    save_json(df, "df_drug.json")


def test_save_hdf():
    save_hdf(df, "df_drug.h5", key="a")


# --------------------------------------------------

# Handling missing data
def test_isna():
    isna(df, cname="review")


def test_dropna():
    dropna(df)


def test_fillna():
    fillna(df, val="0")


def test_replace():
    replace(df, cname="review", src="?", dest="X")


# --------------------------------------------------
# Table operations
# df = pd.read_csv('../Datasets/drugs.csv')
def test_drop():
    drop(df, cnameArray=["drugName"])


def test_groupby():
    groupby(df, cname="rating")


def test_concat_dataframes():
    concat_dataframes(df, df_samp)


def test_sort():
    sort(df, "rating")


def test_merge():
    merge(df, df_samp)


# ------------------------------------------
# Statistical operations
def test_count():
    count(df)


def test_sum():
    sum(df, "usefulCount")


def test_mean():
    mean(df["rating"])


def test_min():
    min(df["usefulCount"])


def test_max():
    max(df["usefulCount"])


def test_unique():
    unique(df["condition"])


# df = load_csv(path='../Datasets/drugs.csv')
# drop_column(df, col_names=['age', 'education', 'occupation'])


# # time.sleep(2)


# remove_duplicates(df_with_dup)
# # time.sleep(2)

# # time.sleep(2)

# SUBSET = ['age', 'workclass', 'education', 'sex', 'race']
# SUBSET_A = ['occupation', 'relationship']
# subset(df, SUBSET)
# # time.sleep(2)

# sample(df, 1000)
# # time.sleep(2)
# sample(df, 10000)
# # time.sleep(2)
# sample(df, 20000)
# # time.sleep(2)

# col = ['capital.gain', 'capital.loss', 'hours.per.week']
# # col = ['capital.gain', 'capital.loss']
