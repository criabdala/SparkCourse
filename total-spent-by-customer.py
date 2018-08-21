from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpentByCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    numberID = int(fields[0])
    spent = float(fields[2])
    return (numberID, spent)

input = sc.textFile("file:///sparkcourse/customer-orders.csv")
fields = input.map(parseLine).reduceByKey(lambda x,y: x+y)
cols = fields.collect()

for col in cols:
    print(col)

