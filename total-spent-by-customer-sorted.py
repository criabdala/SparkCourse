from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpentByCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    numberID = int(fields[0])
    spent = float(fields[2])
    return (numberID, spent)

input = sc.textFile("file:///sparkcourse/customer-orders.csv")
totalByCustomer  = input.map(parseLine).reduceByKey(lambda x,y: x+y)
#totalByCustomerSorted = totalByCustomer.map(lambda x: (x[1], x[0])).sortByKey().map(lambda x: (x[1], x[0]))
totalByCustomerSorted = totalByCustomer.sortBy(lambda x: x[1])

results = totalByCustomerSorted.collect()

for result in results:
    print(result)

