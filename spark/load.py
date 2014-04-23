import sys
from operator import add
from pyspark import SparkContext
import happybase

HBASE_SERVER_IP = '192.168.1.108'
HBASE_FOLDER = '/tmp/hbase/'
OUTPUT_DIR = '/home/mikel/doctorado/src/LDClassifier-python/LDClassifier-python/spark/output'

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
        connection.close()
        return striple[0]
    connection.close()

# There are no literals in input datasets
def generate_edges(uri, graph_table_name, table_name, ac_edge_id):
    connection = happybase.Connection(HBASE_SERVER_IP)
    table = connection.table(table_name)
    graph_table = connection.table(graph_table_name)

    predicates = table.row(uri, columns=['p'])
    objects = table.row(uri, columns=['o'])
    for key in predicates:
        if predicates[key] != '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>':
            obj = objects[key.replace('p', 'o')]
            target_id = graph_table.row(obj, columns=['cf:id'])
            if target_id:
                source_id = graph_table.row(uri, columns=['cf:id'])
                current_edge_id = ac_edge_id
                ac_edge_id.add(1)
                graph_table.put(str(ac_edge_id), {'cf:source': source_id['cf:id'], 'cf:target': target_id['cf:id'], 'cf:label': predicates[key]})

    connection.close()
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: load <master> <file.nt>"
        exit(-1)

    # Getting context and creating accumulators for triples and vertexes
    sc = SparkContext(sys.argv[1], "LoadNT")
    lines = sc.textFile(sys.argv[2], 1)
    ac = sc.accumulator(0)
    ac_vertex_id = sc.accumulator(1)

    # Creating tables
    chunks = sys.argv[2].split('/')
    table_name = chunks[len(chunks) - 1].replace('.nt', '') + '-triples'
    graph_table_name = chunks[len(chunks) - 1].replace('.nt', '') + '-graph'

    connection = happybase.Connection(HBASE_SERVER_IP)
    connection.create_table(table_name, {'p': dict(), 'o': dict()})
    connection.create_table(graph_table_name, {'cf': dict()})

    # # Saving triples and generating vertexes
    counts = lines.map(lambda x: save_triples(x, ac, table_name, ac_vertex_id, graph_table_name))
    output = counts.collect()
    output_file = open('%s/%s' % (OUTPUT_DIR, chunks[len(chunks) - 1].replace('.nt', '')), 'w')
    for uri in output:
        if uri != None:
            output_file.write(uri + '\n')
    output_file.close()

    # Generating edges
    ac_edge_id = sc.accumulator(0)
    lines = sc.textFile('%s/%s' % (OUTPUT_DIR, chunks[len(chunks) - 1].replace('.nt', '')))
    counts = lines.map(lambda x: generate_edges(x, graph_table_name, table_name, ac_edge_id))
    output = counts.collect()

    connection.close()