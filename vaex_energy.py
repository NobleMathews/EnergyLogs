import time
from pickletools import read_uint1
from random import sample

import vaex as ve
from pyJoules.energy_meter import measure_energy
from pyJoules.handler.csv_handler import CSVHandler

df = None
df_samp = None


def reset_env():
    global df, df_samp
    df = ve.read_csv("Datasets/drugs.csv")
    df_samp = ve.read_csv("Datasets/drugs.csv")


# I/O functions - READ
def load_csv(path):
    return ve.read_csv(path)


def load_hdf(path):
    return ve.open(path)


def load_json(path):
    return ve.from_json(path)


# I/O functions - WRITE
def save_csv(df, path):
    return df.export_csv(path)


def save_hdf(df, path, key="a"):
    return df.export(path)


def save_json(df, path):
    return df.export_json(path)


###------------------------------------------###

# Missing data handling
def dropna(df):
    return df.dropna()


def fillna(df, val):
    return df.fillna(val)


def isna(df, cname):
    return df[cname].isna()


def replace(df, cname, src, dest):
    return df[cname].str.replace(src, dest)


###------------------------------------------###

# Table operations
# drop column
# groupby
# merge
# transpose
# sort


def drop(df, col_names=[]):
    df.drop(columns=col_names)


def groupby(df, cname):
    return df.groupby(cname)


def merge(df1, df2, on=None, lsuffix="_l"):
    if on == None:
        return df1.join(df2, lsuffix="_l")
    else:
        return df1.join(df2, on=on, how="inner", lsuffix=lsuffix)


def sort(df, cname):
    return df.sort(cname)


def concat_dataframes(df1, df2):
    return ve.concat([df1, df2])


###--------------------------------------------###
# Statistical Operations
# min, max, mean, count, unique, correlation

# count
def count(df):
    return df.count()


# sum
def sum(df, cname):
    return df.sum(cname)


# mean
def mean(df):
    return df.mean()


# min
def min(df):
    return df.min()


def max(df):
    return df.max()


def unique(df):
    return df.unique()


# # subset
# def subset(df, subset):
#     return df[subset]

# def sample(df, cnt=1000):
#     return df.sample(cnt)


# df = load_csv('Datasets/adult.csv')
# drop_column(df, col_names=['age', 'education', 'occupation'])

# dfa = ve.from_csv('Datasets/adult.csv')
# dfb = ve.from_csv('Datasets/adult.csv')
# concat_dataframes(dfa, dfb)


# SUBSET = ['age', 'workclass', 'education', 'sex', 'race']
# SUBSET_A = ['occupation', 'relationship']
# subset(df, SUBSET)

# sample(df, 1000)
# sample(df, 10000)
# sample(df, 20000)


# col = ['capital.gain', 'capital.loss', 'hours.per.week']
# # col = ['capital.gain', 'capital.loss']
# sum(df[col])
# mean(df, 'age')
# min(df['capital.gain'])
# max(df['capital.gain'])
# unique(df['age'])

# Input Output functions
df = load_csv(path="Datasets/drugs.csv")
## df = load_json(path='Datasets/drugs.json')
## df = load_hdf(path='Datasets/drugs_vaex.hdf5')


def test_save_csv():
    save_csv(df, "df_v.csv")


def test_save_json():
    save_json(df, "df_v.json")


def test_save_hdf():
    save_hdf(df, "df.hdf5")


# --------------------------------------------------

# Handling missing data
df = ve.read_csv("Datasets/drugs.csv")


def test_isna():
    isna(df, cname="review")


def test_dropna():
    dropna(df)


def test_fillna():
    fillna(df, val="0")


def test_replace():
    replace(df, cname="review", src="?", dest="X")


#
# --------------------------------------------------
# Table operations
df = ve.read_csv("Datasets/drugs.csv")
df_samp = ve.read_csv("Datasets/drugs.csv")


def test_drop():
    drop(df, col_names=["drugName"])


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
df = ve.read_csv("Datasets/drugs.csv")


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
