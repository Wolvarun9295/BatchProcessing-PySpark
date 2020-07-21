from pyspark import SparkContext
import time


def main():
    time.sleep(10)
    sc = SparkContext(appName='SparkWordCount')
    time.sleep(10)
    input_file = sc.textFile('input.txt')
    time.sleep(10)
    counts = input_file.flatMap(lambda line: line.split()) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    time.sleep(10)
    counts.saveAsTextFile('output')
    time.sleep(30)
    sc.stop()


if __name__ == '__main__':
    # time.sleep(10)
    main()
