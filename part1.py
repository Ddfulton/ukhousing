#%% Imports
import pandas as pd
import polars as pl
pl.Config.set_tbl_rows(30)

#%% Load data
df = pl.read_csv('pp-2021.csv', has_header=False)
print(df.shape[0], 'transactions')

col_names = [
    'hash',
    'px',
    'date',
    'postcode',
    'c5',
    'c6',
    'c7',
    'houseno',
    'flatno',
    'streetname',
    'c11',
    'town',  # ...or village
    'city',  # ...or town
    'county',
    'c15',
    'c16']

df.columns = col_names

df = df.select([
    pl.col('date').apply(pd.to_datetime),
    pl.all().exclude('date')])

#%% Ensure London is most expensive
px_per_county = df.groupby('county').agg(
    pl.col('px').mean()).sort('px', reverse=True)

#%% Plot by month: n of sales, mean px, median px in SW
# Dorset, Devon, Wiltshire, Somerset, Cornwall, Isle of Wight
sw_counties = ['DORSET', 'DEVON', 'WILTSHIRE', 'SOMERSET', 'CORNWALL', 'ISLE OF WIGHT']
sw = df.filter(
    pl.col('county').is_in(sw_counties)
).with_column(
    pl.col('date').apply(lambda x: x.month).alias('month'))

sw_agg = sw.groupby('month').agg([
    pl.col('county').count().alias('n_sales'),
    (pl.col('px').mean() // 1000).cast(int).alias('px_mean'),
    (pl.col('px').median() // 1000).cast(int).alias('px_median')]).sort('month')



















































