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


df_th1 = spark.read.parquet('s3a://ada-dev/Ampi/device_switchers/TH/202101/top/*')
df_th1 = df_th1.withColumn('month',F.lit(1))


df_th2 = spark.read.parquet('s3a://ada-dev/Ampi/device_switchers/TH/202102/top/')
df_th2 = df_th2.withColumn('month',F.lit(2))

union1 = df_th1.union(df_th2)


df_th3 = spark.read.parquet('s3a://ada-dev/Ampi/device_switchers/TH/202103/top/')
df_th3 = df_th3.withColumn('month',F.lit(3))


# Getting primary dataframe

df = union1.union(df_th3)

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

#Sample of first primary table (df_p5)

#df_p5.take(1)

#[Row(new_ifa='2db7448d-97a4-4bb1-83b5-b5bb6538401e', old_ifa='02b090f7-518f-4e5d-b0df-c38c0393305d', old_device_vendor='Vivo', old_device_name='Vivo Y11 (2019)', old_device_manufacturer='Vivo', old_device_model='1906', old_platform='Android', old_major_os='Android 9.0', old_device_category='Smartphone', old_price=129.0, old_pricegrade='3', new_device_vendor='Apple', new_device_name='Apple iPhone', new_device_manufacturer='Apple', new_device_model='iPhone', new_platform='iOS', new_major_os='iOS 14.3', new_device_category='Smartphone', new_price=1150.0, new_pricegrade='1', old_device_year_of_mfg='2019', new_device_year_of_mfg='2017')]



# Get TMI DataFrame and join with df_p5

months = ['202101','202102','202103']

for month in months:
    tmi_path = 's3a://ada-geogrid-feature-set/telco_market_insight/high_quantity/TH/{}/'.format(month)
    df_tmi = spark.read.parquet(tmi_path)
    df_tmi = df_tmi.select('ifa','gender','age_cat','cellular_carriers','connection_type')
    output = 's3a://ada-dev/ishti/tmi_th/{}'.format(month)
    df_tmi.write.format('parquet').option('compression','snappy').save(output)

#aggregated dataframe

df_tmiagg = spark.read.parquet('s3a://ada-dev/ishti/tmi_th/*')


# note the diff between tmi and tmiagg

df_p6 = df_p5.join(df_tmiagg, df_p5.old_ifa==df_tmiagg.ifa, how='left')


# Get affluence Data (No affluence data for thailand)

#aff_path = 's3a://ada-prod-data/etl/data/brq/sub/affluence/monthly/TH/202101/'
#df_aff = spark.read.parquet(aff_path)
#df_aff = df_aff.select('ifa','final_affluence')
#df_p7 = df_p6.join(df_aff, df_p6.old_ifa==df_aff.ifa, how='left').withColumnRenamed('final_affluence','affluence')


# lifestage

lsl = ['working_adult','single','parents_with_teens_(13-17)', 'parents_with_kids_(7-12)', 'parents_with_kids_(0-6)', 'in_a_relationship_married', 'expecting_parents', 'college_university_student']
months = ['202011','202012','202101']

for ls in lsl:
    for month in months:
        path = 's3a://ada-dev/DA_repository/lifestage/TH/{}/{}/ifa_list/'.format(ls,month)
        df = spark.read.parquet(path)
        df.write.format('parquet').option('compression','snappy').save('s3a://ada-dev/ishti/ls_th_3m/{}/{}'.format(month,ls))


df_ls1 = spark.read.parquet('s3a://ada-dev/ishti/ls_th_3m/202011/*')

df_ls2 = spark.read.parquet('s3a://ada-dev/ishti/ls_th_3m/202012/*')

df_lsu1 = df_ls1.union(df_ls2)

df_ls3 = spark.read.parquet('s3a://ada-dev/ishti/ls_th_3m/202101/*')

df_ls = df_lsu1.union(df_ls3)

df_p7 = df_p6.join(df_ls, df_p6.old_ifa==df_ls.ifa, how='left')

# home location

hl_path = 's3a://ada-prod-data/etl/data/brq/sub/home-office/home-office-data/TH/202106/home-location/'
df_hl = spark.read.parquet(hl_path)
df_hl1 = df_hl.withColumn('home_geohash6',df_hl.home_geohash[0:6]).select('ifa','home_geohash6','home_province', 'home_district', 'home_sub-district')


>>> df_hl.printSchema()
root
 |-- home_geohash: string (nullable = true)
 |-- ifa: string (nullable = true)
 |-- home_latitude: double (nullable = true)
 |-- home_longitude: double (nullable = true)
 |-- home_total_month: long (nullable = true)
 |-- home_total_days: long (nullable = true)
 |-- home_total_weekdays: integer (nullable = true)
 |-- home_total_hour: long (nullable = true)
 |-- home_weekdays: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- home_average_days/month: double (nullable = true)
 |-- home_country: string (nullable = true)
 |-- home_province: string (nullable = true)
 |-- home_district: string (nullable = true)
 |-- home_sub-district: string (nullable = true)


# Second last joining

df_p8 = df_p7.join(df_hl1,df_p7.old_ifa==df_hl1.ifa,how='left')
df_p8 = df_p8.drop('ifa','ifa','ifa')

# flatten the carrier Column()

df_q1 = df_p8.select('old_ifa',F.explode('cellular_carriers'))

df_q2 = df_q1.withColumnRenamed('col','carriers')

df_q3 = df_p8.join(df_q2, on = 'old_ifa', how='left')

df_q4 = df_q3.drop('cellular_carriers')

df_q5 = df_q4.withColumnRenamed('carriers','cellular_carriers')

df_q6 = df_q5.withColumnRenamed('old_month','month').drop('new_month')

# Output file

df_q6.write.format('parquet').option('compression','snappy').save('s3a://ada-dev/ishti/thai_3month_data_v3')

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
