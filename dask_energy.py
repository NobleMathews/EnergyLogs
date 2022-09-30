import time
from pickletools import read_uint1
from random import sample

import dask.dataframe as pd
import pandas
from pyJoules.energy_meter import measure_energy
from pyJoules.handler.csv_handler import CSVHandler

df = None
df_samp = None


def reset_env():
    global df, df_samp
    df = pandas.read_csv("Datasets/drugs.csv")
    df_samp = pandas.read_csv("Datasets/drugs.csv")

    df = pd.from_pandas(df, npartitions=3)
    df_samp = pd.from_pandas(df_samp, npartitions=3)


# I/O functions - READ
def load_csv(path):
    return pd.read_csv(path)


def load_hdf(path, key):
    return pd.read_hdf(path, key=key)


def load_json(path):
    return pd.read_json(path, orient=str)


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
    return df.count().compute()


# sum
def sum(df, cname):
    return df[cname].sum().compute()


# mean
def mean(df):
    return df.mean().compute()


# min
def min(df):
    return df.min().compute()


# max
def max(df):
    return df.max().compute()


# unique
def unique(df):
    return df.unique().compute()


# # def drop_column(df, col_names=[]):
#     df.drop(columns=col_names)

# # def remove_duplicates(df):
#     return df.drop_duplicates()

# # def merge(df1, df2, on=None):
#     if(on):
#         return pd.merge(df1, df2, on=on)
#     else:
#         return pd.merge(df1, df2)

# # def group_by(df, groupby):
#     pass

# # subset
# # def subset(df, subset):
#     return df[subset]

# # sample
# # def sample(df, cnt=1000):
#     return df.sample(cnt)

# count, mean, min, max, value_counts, unique, sort values, groupby

# Input output functions
df = load_csv(path="Datasets/drugs.csv")
## df = load_json(path='Datasets/drugs.json')
# df = load_hdf(path='Datasets/drugs_dask.hdf', key='a')
#


def test_save_csv():
    save_csv(df, "df.csv")


def test_save_json():
    save_json(df, "df.json")


# save_hdf():
# save_hdf(df, 'df.hdf', key='a')

# --------------------------------------------------

# Handling missing data
df = pd.read_csv("Datasets/drugs.csv")


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
df = pd.read_csv("Datasets/drugs.csv")
df_samp = pd.read_csv("Datasets/drugs.csv")


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
df = pd.read_csv("Datasets/drugs.csv")


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


# df = load_csv(path='Datasets/adult.csv')
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
