import json
import time
import shutil
import pyspark


def sentiments(tweet):
    words = tweet['text'].split(" ")
    ratio = 0.0

    for word in words:
        if len(word) <= 0 or word[0] == '#':
            continue

        if word in positive_words:
            ratio -= 1

        if word in negative_words:
            ratio += 1

    return [(hashtag, (ratio / float(len(tweet['text'])))) for hashtag in tweet['hashtags']]


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
        lambda i: {"text": i["text"], "lang": i["lang"],
                   "hashtags": [j['text'] for j in i["entities"]["hashtags"]]}).filter(lambda i: "es" in i["lang"])

    # Trending Topics
    # https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.RDD.flatMap.html
    trending_data = clean_data.flatMap(lambda i: i['hashtags']).map(lambda hashtag: (hashtag, 1)).reduceByKey(
        lambda x, y: x + y)

    topn = sc.parallelize(trending_data.takeOrdered(10, lambda t: -t[1]))
    topn.saveAsTextFile("out")

    positive_words = set(line.strip().lower() for line in open("input/data/positive_words_es.txt"))
    negative_words = set(line.strip().lower() for line in open("input/data/negative_words_es.txt"))

    sentiments = clean_data.flatMap(lambda i: sentiments(i)).collect()

    print(sc.parallelize(sentiments).reduceByKey(lambda x, y: x + y).collect())

    print((time.time() - start_time) * 1000)
