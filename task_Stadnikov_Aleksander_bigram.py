from pyspark import SparkConf, SparkContext
import re

conf = SparkConf()
sc = SparkContext(master="yarn", conf=conf)

wiki = sc.textFile('/data/wiki/en_articles_part/articles-part')\
         .map(lambda x: x.split("\t")[1].lower())
wiki2 = wiki.flatMap(lambda x: re.findall(r"(narodnaya)\s+(\w+)", x))\
            .map(lambda x: '_'.join(x))\
            .map(lambda x: (x, 1))\
            .reduceByKey(lambda x, y: x+y)\
            .sortByKey()\
            .cache()

result = wiki2.collect()

for i in result:
    print(i[0], i[1], sep='\t')
sc.stop()
