import json
import time
import shutil
import pyspark

if __name__ == '__main__':
    start_time = time.time()
    # https://guru99.es/pyspark-tutorial/
    # Create Spark Context
    sc = pyspark.SparkContext('local[*]', appName='SparkActivity')

    # Read Input Files
    # inputFiles = sc.textFile("hdfs:///shared/nando/data/tweets/tweets*.json")
    inputFiles = sc.textFile('input/tweets/tweets.json')
    shutil.rmtree('out/', ignore_errors=True)

    # Typescript: arr.map((item) => item+1)

    lower_data = inputFiles.map(lambda i: json.loads(i.lower()))

    # Clean Fields
    clean_data = lower_data.filter(lambda t: t["entities"]["hashtags"] != []).map(
        lambda t: {"text": t["text"], "lang": t["lang"],
                   "hashtags": [x['text'] for x in t["entities"]["hashtags"]]}).filter(lambda i: "es" in i["lang"])

    # Trending Topics
    trending_data = clean_data.flatMap(lambda h: h['hashtags']).map(lambda hashtag: (hashtag, 1)).reduceByKey(
        lambda a, b: a + b)

    TopN = sc.parallelize(trending_data.takeOrdered(10, lambda t: -t[1]))
    TopN.saveAsTextFile("out")


