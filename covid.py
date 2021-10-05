from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

if __name__ == "__main__":
    conf = SparkConf().setAppName("Top20Country - AccumulationCovidInJuly2020")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    df_fact = sqlContext.read.load('fact_covid.csv',format='csv',header='true',inferSchema='true')
    df_fact = df_fact.filter(df_fact['record_date'] >= '2020-07-01')
    df_fact = df_fact.filter(df_fact['record_date'] <= '2020-07-31')
    df_fact = df_fact.groupBy(df_fact['geo_id']).agg({"daily_confirmed_cases":"sum"}).withColumnRenamed("sum(daily_confirmed_cases)", "top_20_country_daily_confirmed_cases_in_july_2020")

    df_dim = sqlContext.read.load('dim_country_name.csv', \
                          format='csv', \
                          header='true', \
                          inferSchema='true')

    df_joined = df_dim.join(df_fact, on=['geo_id'], how='inner')
    df_joined = df_joined.orderBy(df_fact['top_20_country_daily_confirmed_cases_in_july_2020'].desc()).limit(20)
    df_joined = df_joined[['country_name','top_20_country_daily_confirmed_cases_in_july_2020']]
    df_joined.show()
    df_joined.write.format("csv").save('result_top20.csv', header = True)