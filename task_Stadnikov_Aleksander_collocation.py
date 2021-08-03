from pyspark import SparkConf, SparkContext
import re
import math

conf = SparkConf()
conf.set("spark.ui.port", 20917)

sc = SparkContext(master="yarn", conf=conf)

rdd = sc.textFile('/data/wiki/en_articles_part/')\
    .map(lambda x: x.split('\t')[1].lower())

stop_words = sc.textFile('/data/stop_words/stop_words_en-xpo6.txt')

collocation = rdd.map(lambda x: re.findall(r"\w+", x))


def s_v_p(articles):
    for i in range(len(articles)-1):
        yield (articles[i] + '_' + articles[i + 1])

collocation_new = collocation.flatMap(lambda x: s_v_p(x))

spisok_stopov = stop_words.collect()

col_bez_sl = collocation_new\
    .filter(lambda x: x.split('_')[0] not in spisok_stopov and x.split('_')[1] not in spisok_stopov)\
    .map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).cache()

filter_500 = col_bez_sl.filter(lambda x: x[1] >= 500)

all_words = collocation.flatMap(lambda x: x)
total_number_of_words = all_words.count()
total_number_of_word_pairs = collocation_new.count()
num_of_occurrences_of_word_a = all_words.map(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: x+y).collect()
num_of_occurrences_of_word_a = dict(num_of_occurrences_of_word_a)
num_of_occurrences_of_word_ab = collocation_new.map(lambda x: (x, 1))\
    .reduceByKey(lambda x, y: x+y).cache().collect()
num_of_occurrences_of_word_ab = dict(num_of_occurrences_of_word_ab)


def npmi(a, b):
    P_a = num_of_occurrences_of_word_a[a]/total_number_of_words
    P_b = num_of_occurrences_of_word_a[b]/total_number_of_words
    P_ab = num_of_occurrences_of_word_ab[a + '_' + b]
    P_ab /= total_number_of_word_pairs
    PMI = math.log(P_ab/(P_a * P_b))
    NPMI = PMI/-math.log(P_ab)
    return NPMI

result = filter_500\
        .map(lambda x: (npmi(x[0].split('_')[0], x[0].split('_')[1]), x[0]))\
        .sortByKey(False).cache()
res = result.collect()

for i in range(39):
    print(res[i][1], round(res[i][0], 3), sep='\t')

sc.stop()
