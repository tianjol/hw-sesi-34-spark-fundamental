from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RetailData")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(';')

    InvoiceNo = str(fields[0])
    Description = str(fields[1])
    Amount = float(fields[2])
    CustomerID = str(fields[3])
    
    return (CustomerID, Amount)

lines = sc.textFile("retail-data.csv")

rdd = lines.map(parseLine)
rdd.collect()
AmountsByCstID = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
AmountsByCstID.collect()
averagesByCstID = AmountsByCstID.mapValues(lambda x: x[0] / x[1])
results = averagesByCstID.collect()
for result in results:
    print(result)