import sys
from operator import add
from pyspark import SparkContext
import happybase

HBASE_SERVER_IP = '192.168.1.108'

def save_triples(triple, ac, table_name, ac_vertex_id, graph_table_name):
    striple = triple.split(' ')
    connection = happybase.Connection(HBASE_SERVER_IP)
    table = connection.table(table_name)
    current = ac
    ac.add(1)
    pred = 'p:%s' % current
    obj = 'o:%s' % current
    table.put(striple[0], {pred: striple[1], obj: striple[2]})
    if striple[1] == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>':
        graph_table = connection.table(graph_table_name)
        current_vertex_id = ac_vertex_id
        ac_vertex_id.add(1)
        graph_table.put(striple[0], {'cf:id': str(current_vertex_id), 'cf:label': striple[2], 'cf:type': 'v'})
    #return ('triple', 1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: load <master> <file.nt>"
        exit(-1)

    sc = SparkContext(sys.argv[1], "LoadNT")
    lines = sc.textFile(sys.argv[2], 1)
    ac = sc.accumulator(0)
    ac_vertex_id = sc.accumulator(1)

    chunks = sys.argv[2].split('/')
    table_name = chunks[len(chunks) - 1].replace('.nt', '') + '-triples'
    graph_table_name = chunks[len(chunks) - 1].replace('.nt', '') + '-graph'

    connection = happybase.Connection(HBASE_SERVER_IP)
    connection.create_table(table_name, {'p': dict(), 'o': dict()})
    connection.create_table(graph_table_name, {'cf': dict()})

    counts = lines.map(lambda x: save_triples(x, ac, table_name, ac_vertex_id, graph_table_name))
    output = counts.collect()
    # for (word, count) in output:
    #     print "%s: %i" % (word, count)
