import sys
from operator import add
from pyspark import SparkContext
import happybase

HBASE_SERVER_IP = '192.168.1.108'

def save_triples(triple, ac, table_name):
    striple = triple.split(' ')
    connection = happybase.Connection(HBASE_SERVER_IP)
    table = connection.table(table_name)
    current = ac
    ac.add(1)
    pred = 'p:%s' % current
    obj = 'o:%s' % current
    table.put(striple[0], {pred: striple[1], obj: striple[2]})
    return ('triple', 1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: load <master> <file.nt>"
        exit(-1)

    sc = SparkContext(sys.argv[1], "LoadNT")
    lines = sc.textFile(sys.argv[2], 1)
    ac = sc.accumulator(0)

    chunks = sys.argv[2].split('/')
    table_name = chunks[len(chunks) - 1].replace('.nt', '') + '-triples'

    connection = happybase.Connection(HBASE_SERVER_IP)
    connection.create_table(table_name, {'p': dict(), 'o': dict()})

    counts = lines.map(lambda x: save_triples(x, ac, table_name))
    output = counts.collect()
    # for (word, count) in output:
    #     print "%s: %i" % (word, count)
