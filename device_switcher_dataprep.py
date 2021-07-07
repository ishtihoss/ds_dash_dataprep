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

>>> df.printSchema()
root
 |-- old_ifa: string (nullable = true)
 |-- new_ifa: string (nullable = true)
 |-- device_new: struct (nullable = true)
 |    |-- device_vendor: string (nullable = true)
 |    |-- device_name: string (nullable = true)
 |    |-- device_manufacturer: string (nullable = true)
 |    |-- device_model: string (nullable = true)
 |    |-- device_year_of_release: string (nullable = true)
 |    |-- platform: string (nullable = true)
 |    |-- major_os: string (nullable = true)
 |    |-- device_category: string (nullable = true)
 |    |-- price: float (nullable = true)
 |    |-- pricegrade: string (nullable = true)
 |-- device_old: struct (nullable = true)
 |    |-- device_vendor: string (nullable = true)
 |    |-- device_name: string (nullable = true)
 |    |-- device_manufacturer: string (nullable = true)
 |    |-- device_model: string (nullable = true)
 |    |-- device_year_of_release: string (nullable = true)
 |    |-- platform: string (nullable = true)
 |    |-- major_os: string (nullable = true)
 |    |-- device_category: string (nullable = true)
 |    |-- price: float (nullable = true)
 |    |-- pricegrade: string (nullable = true)




# Getting primary dataframe

df_path = 's3a://ada-dev/Ampi/device_switchers/MY/202101/top/'
df = spark.read.parquet(df_path)

# Get old device values

df1 = df.select(F.col('device_old.*'),'old_ifa')


# Get new device values

df2 = df.select(F.col('device_new.*'),'new_ifa')

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

#Sample of first primary table (df_p5)

#df_p5.take(1)

#[Row(new_ifa='2db7448d-97a4-4bb1-83b5-b5bb6538401e', old_ifa='02b090f7-518f-4e5d-b0df-c38c0393305d', old_device_vendor='Vivo', old_device_name='Vivo Y11 (2019)', old_device_manufacturer='Vivo', old_device_model='1906', old_platform='Android', old_major_os='Android 9.0', old_device_category='Smartphone', old_price=129.0, old_pricegrade='3', new_device_vendor='Apple', new_device_name='Apple iPhone', new_device_manufacturer='Apple', new_device_model='iPhone', new_platform='iOS', new_major_os='iOS 14.3', new_device_category='Smartphone', new_price=1150.0, new_pricegrade='1', old_device_year_of_mfg='2019', new_device_year_of_mfg='2017')]



# Get TMI DataFrame and join with df_p5

tmi_path = 's3a://ada-geogrid-feature-set/telco_market_insight/high_quantity/MY/202101/'
df_tmi = spark.read.parquet(tmi_path)
df_tmi = df_tmi.select('ifa','gender','age_cat','cellular_carriers','connection_type')
df_p6 = df_p5.join(df_tmi, df_p5.old_ifa==df_tmi.ifa, how='left')


# Get affluence Data

aff_path = 's3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/MY/202101/'
df_aff = spark.read.parquet(aff_path)
df_aff = df_aff.select('ifa','final_affluence')
df_p7 = df_p6.join(df_aff, df_p6.old_ifa==df_aff.ifa, how='left').withColumnRenamed('final_affluence','affluence')


# lifestage

lsl = ['working_adult','single','parents_with_teens_(13-17)', 'parents_with_kids_(7-12)', 'parents_with_kids_(0-6)', 'in_a_relationship_married', 'expecting_parents', 'college_university_student']

for ls in lsl:
    path = 's3a://ada-dev/DA_repository/lifestage/MY/{}/202101/ifa_list/'.format(ls)
    df = spark.read.parquet(path)
    df.write.format('parquet').option('compression','snappy').save('s3a://ada-dev/ishti/ls/{}'.format(ls))

ls_path = 's3a://ada-dev/ishti/ls/*'
df_ls = spark.read.parquet(ls_path)

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

# Output file

df_q5.write.format('parquet').option('compression','snappy').save('s3a://ada-dev/ishti/ds_dash2')

# household_id df

#hh_path = 's3a://ada-platform-components/Household/MY/*'
#df_hhid = spark.read.parquet(hh_path)

#df_hh = df_hhid.select('household_id','home_geohash','household_size',F.explode('household_members'))

#df_hh = df_hh.withColumnRenamed('col','ifa')


# geohash

#geo_path = 's3a://ada-prod-data/etl/data/ref/geohash_lookup_table/geohash6/MY/'
#df_geo = spark.read.parquet(geo_path)

# join geohash with household df

#df_hh1 = df_hh.join(df_geo, on='ifa' ,how='left')
