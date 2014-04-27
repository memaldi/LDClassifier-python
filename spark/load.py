import sys
from operator import add
from pyspark import SparkContext
import struct
import happybase
import collections

HBASE_SERVER_IP = 'localhost'
HBASE_FOLDER = '/tmp/hbase/'
OUTPUT_DIR = '/home/mikel/doctorado/src/LDClassifier-python/LDClassifier-python/spark/output'
VERTEX_LIMIT = 1000
SUBDUE_OUTPUT_DIR = '/home/mikel/doctorado/src/LDClassifier-python/LDClassifier-python/spark/output/subdue'

class CustomConnectionPool(object):
    _connection = None
    _instance = None

    def get_connection(self):
        return self._connection

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(CustomConnectionPool, cls).__new__(cls, *args, **kwargs)
            cls._connection = happybase.Connection(HBASE_SERVER_IP, compat='0.94')
        return cls._instance

def save_triples(triple, ac, table_name, ac_vertex_id, graph_table_name):
    print 'save_triples'
    striple = triple.split(' ')
    connection = happybase.Connection(HBASE_SERVER_IP, compat='0.94')
    # connection_pool = CustomConnectionPool()
    # connection = connection_pool.get_connection()
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
        graph_table.put(striple[0], {'cf:id': struct.pack(">q", int(str(ac_vertex_id))), 'cf:label': striple[2], 'cf:type': 'v'})
        connection.close()
        print 'connection closed'
        return striple[0]
    connection.close()
    print 'connection close'

# There are no literals in input datasets
def generate_edges(uri, graph_table_name, table_name, ac_edge_id):
    connection = happybase.Connection(HBASE_SERVER_IP, compat='0.94')
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
                graph_table.put(str(ac_edge_id), {'cf:source': source_id['cf:id'], 'cf:target': target_id['cf:id'], 'cf:label': predicates[key], 'cf:type': 'e'})

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

    #connection = happybase.Connection(HBASE_SERVER_IP, compat='0.94')
    connection_pool = CustomConnectionPool()
    connection = connection_pool.get_connection()
    connection.create_table(table_name, {'p': dict(), 'o': dict()})
    connection.create_table(graph_table_name, {'cf': dict()})

    # Saving triples and generating vertexes
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

    # Writing output file

    stop = False
    graph_table = connection.table(graph_table_name)
    total = 0
    for key, data in graph_table.scan(filter="SingleColumnValueFilter ('cf', 'type', =, 'binary:v')"):
        total += 1
    limit = VERTEX_LIMIT
    offset = 1
    while(not stop):
        if (limit > total):
            limit = total + 1
            stop = True

        f = open('%s/%s-%s' % (SUBDUE_OUTPUT_DIR, offset, limit-1), 'w')

        # Vertexes
        vertex_dict = {}
        for key, data in graph_table.scan(filter="SingleColumnValueFilter('cf', 'id', >=, 'binary:%s', true, false) AND SingleColumnValueFilter('cf', 'id', <, 'binary:%s', true, false)" % (struct.pack(">q", offset), struct.pack(">q", limit))):
            vertex_dict[struct.unpack(">q", data['cf:id'])[0]] = data['cf:label']
        ordered_dict = collections.OrderedDict(sorted(vertex_dict.items()))
        for key in ordered_dict:
            f.write('v %s %s\n' % (key, ordered_dict[key]))

        #Edges
        for key, data in graph_table.scan(filter="SingleColumnValueFilter('cf', 'source', <, 'binary:%s', true, false) AND SingleColumnValueFilter('cf', 'target', <, 'binary:%s', true, false)" % (struct.pack(">q", limit), struct.pack(">q", limit))):
            f.write('e %s %s %s\n' % (struct.unpack(">q", data['cf:source'])[0], struct.unpack(">q", data['cf:target'])[0], data['cf:label']))
        f.close()
        offset = limit
        limit += VERTEX_LIMIT

    connection.close()