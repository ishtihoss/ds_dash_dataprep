######### Device Switcher Dashboard Data Prep ##########

# Load necessary packages

from pyspark import SparkContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType
from pyspark.sql.functions import explode
from pyspark.sql.functions import regexp_replace


# consolidating 3 months data after adding month column values


df_my1 = spark.read.parquet('s3a://ada-dev/Ampi/device_switchers/MY/202101/top/')
df_my1 = df_my1.withColumn('month',F.lit(1))


df_my2 = spark.read.parquet('s3a://ada-dev/Ampi/device_switchers/MY/202102/top/')
df_my2 = df_my2.withColumn('month',F.lit(2))

union1 = df_my1.union(df_my2)


df_my3 = spark.read.parquet('s3a://ada-dev/Ampi/device_switchers/MY/202103/top/')
df_my3 = df_my3.withColumn('month',F.lit(3))

#Getting primary dataframe

df = union1.union(df_my3)

# Get old device values

df1 = df.select(F.col('device_old.*'),'old_ifa','month')


# Get new device values

df2 = df.select(F.col('device_new.*'),'new_ifa', 'month')

# Get flattened old and new vals

df_oldvals = df1.select([F.col(c).alias("old_"+c) for c in df1.columns])
df_oldvals = df_oldvals.withColumnRenamed('old_old_ifa','old_ifa')

df_newvals = df2.select([F.col(c).alias("new_"+c) for c in df2.columns])
df_newvals = df_newvals.withColumnRenamed('new_new_ifa','new_ifa')


# ifa pair df creation the joining old and new device vals

ifa_pair = df.select('old_ifa','new_ifa')

# joining old device values

df_p1 = ifa_pair.join(df_oldvals,on='old_ifa',how ='left')

# joining new device values to df_p1

df_p2 = df_p1.join(df_newvals, on='new_ifa', how = 'left')

# Clean up the year column

df_p3 = df_p2.withColumn('old_device_year_of_mfg',regexp_replace(df_p2.old_device_year_of_release, '[a-z_]',''))
df_p4 = df_p3.withColumn('new_device_year_of_mfg',regexp_replace(df_p2.new_device_year_of_release, '[a-z_]',''))
df_p5 = df_p4.drop('old_device_year_of_release','new_device_year_of_release')

# Get TMI DataFrame and join with df_p5

months = ['202101','202102','202103']

for month in months:
    tmi_path = 's3a://ada-geogrid-feature-set/telco_market_insight/high_quantity/MY/{}/'.format(month)
    df_tmi = spark.read.parquet(tmi_path)
    df_tmi = df_tmi.select('ifa','gender','age_cat','cellular_carriers','connection_type')
    output = 's3a://ada-dev/ishti/tmi_my/{}'.format(month)
    df_tmi.write.format('parquet').option('compression','snappy').save(output)

#aggregated dataframe

df_tmiagg = spark.read.parquet('s3a://ada-dev/ishti/tmi_my/*')


# note the diff between tmi and tmiagg

df_p6 = df_p5.join(df_tmiagg, df_p5.old_ifa==df_tmiagg.ifa, how='left')


# Get affluence Data

months = ['202101','202102','202103']

for month in months:
    aff_path = 's3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/MY/{}/'.format(month)
    df_aff = spark.read.parquet(aff_path)
    df_aff = df_aff.select('ifa','final_affluence')
    output = 's3a://ada-dev/ishti/aff_my/{}'.format(month)
    df_aff.write.format('parquet').option('compression','snappy').save(output)

# aggregated affluence for MY

df_aff_agg = spark.read.parquet('s3a://ada-dev/ishti/aff_my/*')

# join affluence data

df_p7 = df_p6.join(df_aff_agg, df_p6.old_ifa==df_aff_agg.ifa, how='left').withColumnRenamed('final_affluence','affluence')

# lifestage loop

lsl = ['working_adult','single','parents_with_teens_(13-17)', 'parents_with_kids_(7-12)', 'parents_with_kids_(0-6)', 'in_a_relationship_married', 'expecting_parents', 'college_university_student']
months = ['202101','202102','202103']

for ls in lsl:
    for month in months:
        path = 's3a://ada-dev/DA_repository/lifestage/MY/{}/{}/ifa_list/'.format(ls,month)
        df = spark.read.parquet(path)
        df.write.format('parquet').option('compression','snappy').save('s3a://ada-dev/ishti/ls_my_3m/{}/{}'.format(month,ls))


df_ls1 = spark.read.parquet('s3a://ada-dev/ishti/ls_my_3m/202101/*')

df_ls2 = spark.read.parquet('s3a://ada-dev/ishti/ls_my_3m/202102/*')

df_lsu1 = df_ls1.union(df_ls2)

df_ls3 = spark.read.parquet('s3a://ada-dev/ishti/ls_my_3m/202103/*')

df_ls = df_lsu1.union(df_ls3)

df_p8 = df_p7.join(df_ls, df_p7.old_ifa==df_ls.ifa, how='left')

# home location

hl_path = 's3a://ada-prod-data/etl/data/brq/sub/home-office/home-office-data/MY/202106/home-location/'
df_hl = spark.read.parquet(hl_path)
df_hl1 = df_hl.withColumn('home_geohash6',df_hl.home_geohash[0:6]).select('ifa','home_geohash6','home_state', 'home_district', 'home_parlimen')


# Second last joining

df_p9 = df_p8.join(df_hl1,df_p8.old_ifa==df_hl1.ifa,how='left')
df_p9 = df_p9.drop('ifa','ifa','ifa')

# flatten the carrier Column()

df_q1 = df_p9.select('old_ifa',F.explode('cellular_carriers'))

df_q2 = df_q1.withColumnRenamed('col','carriers')

df_q3 = df_p9.join(df_q2, on = 'old_ifa', how='left')

df_q4 = df_q3.drop('cellular_carriers')

df_q5 = df_q4.withColumnRenamed('carriers','cellular_carriers')

df_q6 = df_q5.withColumnRenamed('old_month','month').drop('new_month')

# Output file

df_q6.write.format('parquet').option('compression','snappy').save('s3a://ada-dev/ishti/my_3month_data_v3')
