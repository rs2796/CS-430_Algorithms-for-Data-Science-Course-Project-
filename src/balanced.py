import random
import time
import re
from operator import add
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("test")
sc = SparkContext(conf = conf)

k = 4	# number of partitions
part = []
for i in range(k):
	part.append(0)

test_file_name = "/home/arik/graph-partitioning/test.txt"
file_name = "/home/arik/graph-partitioning/database/Amazon0302.txt"

lines = sc.textFile(file_name)
def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)
def conv(x):
	x = x.split()
	return (int(x[0]), [int(x[1])])

edges = lines.map(lambda x: conv(x))
# print edges.collect()

# number of nodes
max_node = edges.max(lambda x: max(x[0], x[1]))
max_vertex = max_node[0]
# print max_vertex

# create an Adjacency list of the graph
AL = edges.reduceByKey(lambda a, b: a+b)
AL.persist()	# persist the AL RDD
AL_list = AL.collect()

# max_vertex -> maximum node id
# d -> dictionary (key = vertex of the graph, value = partition node)
d = {i:-1 for i in range(0, max_vertex+1)}
# print d

def graph_partition(v, k):
	minm = 999999
	for i in range(0, len(part)):
		if part[i] < minm:
			minm = part[i]
	choices = []	# all partitions with minimum number of partitions
	for i in range(0, len(part)):
		if(part[i] == minm):
			choices.append(i)
	# choose a random partition from the above choices 
	x = random.choice(choices)
	part[x] += 1
	d[v] = x
	return d[v]

# print d
# partition the nodes with graph_partition method
# wp -> AL (RDD) partitioned
wp = AL.partitionBy(k, lambda v: graph_partition(v, k))
# implement page rank algorithm 
ranks = wp.map(lambda x: (x[0], 1.0))
# print ranks.collect()
x = wp.join(ranks)
# print x.collect()
number_of_iterations = 50
for iteration in range(int(number_of_iterations)):
	contribs = x.flatMap(lambda x: computeContribs(x[1][0], x[1][1]))
	ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

for (link, rank) in ranks.collect():
	print ("%s has rank: %s" % (link, rank))
# print (time.time() - start_time)
sc.stop()
# PAGERANK #



