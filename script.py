import sys
import struct
import happybase
import collections
import logging

HBASE_SERVER_IP = 'localhost'
VERTEX_LIMIT = 1000
SUBDUE_OUTPUT_DIR = '/home/mikel/doctorado/src/LDClassifier-python/LDClassifier-python/output/subdue'
LOGGER_FORMAT =     '[%(asctime)s] - %(message)s'

def escape(s):
    """Escape a byte string for use in a filter string."""

    if not isinstance(s, bytes):
        raise TypeError("Only byte strings can be escaped")

    return s.replace(b"'", b"''")

def save_triples(table, graph_table, input_file):
    current = 1
    current_vertex_id = 1
    bt = table.batch()
    btg = graph_table.batch()
    with open(input_file) as f:
        for triple in f:
            striple = triple.split(' ')
            pred = 'p:%s' % current
            obj = 'o:%s' % current
            current += 1
            bt.put(striple[0], {pred: striple[1], obj: striple[2]})
            if (current % 10000) == 0:
                print 'Loaded %s triples...' % current
		sys.stdout.flush()
                bt.send()
                btg.send()
            if striple[1] == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>':
                btg.put(striple[0], {'cf:id': struct.pack(">q", current_vertex_id), 'cf:label': striple[2], 'cf:type': 'v'})
                current_vertex_id += 1
        bt.send()
        btg.send()

def generate_edges(table, graph_table):
    edge_id = 0
    counter = 1
    b = graph_table.batch()
    for uri, data in graph_table.scan(filter="SingleColumnValueFilter ('cf', 'type', =, 'binary:v')"):
        if (counter % 10000) == 0:
            print 'Analyzed %s vertexes...' % counter
	    sys.stdout.flush()
        if (edge_id % 10000) == 0:
            b.send()
        counter += 1
        predicates = table.row(uri, columns=['p'])
        objects = table.row(uri, columns=['o'])
        for key in predicates:
            if predicates[key] != '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>':
                obj = objects[key.replace('p', 'o')]
                target_id = graph_table.row(obj, columns=['cf:id'])
                if target_id:
                    source_id = data['cf:id']
                    b.put(str(edge_id), {'cf:source': source_id, 'cf:target': target_id['cf:id'], 'cf:label': predicates[key], 'cf:type': 'e'})
                    edge_id += 1
    b.send()
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: load <master> <file.nt>"
        exit(-1)

    # logging.basicConfig(format=LOGGER_FORMAT)
    # logger = logging.getLogger('LD2SUBDUE')
    # logger.info('Initializing...')

    print 'Initializing...'
    sys.stdout.flush()
    chunks = sys.argv[2].split('/')
    table_name = chunks[len(chunks) - 1].replace('.nt', '') + '-triples'
    graph_table_name = chunks[len(chunks) - 1].replace('.nt', '') + '-graph'

    connection = happybase.Connection(HBASE_SERVER_IP, compat='0.94')
    connection.create_table(table_name, {'p': dict(), 'o': dict()})
    connection.create_table(graph_table_name, {'cf': dict()})

    table = connection.table(table_name)
    graph_table = connection.table(graph_table_name)

    print 'Loading triples and generating vertexes...'
    sys.stdout.flush()
    save_triples(table, graph_table, sys.argv[2])
    print 'Generating Edges...'
    sys.stdout.flush()
    generate_edges(table, graph_table)

    print 'Writing output files...'
    sys.stdout.flush()
    stop = False
    total = 0
    counter = 1
    for key, data in graph_table.scan(filter="SingleColumnValueFilter ('cf', 'type', =, 'binary:v')"):
        total += 1
    #print total
    limit = VERTEX_LIMIT
    offset = 1
    while(not stop):
        if (limit > total):
            limit = total + 1
            stop = True

        f = open('%s/%s_%s.g' % (SUBDUE_OUTPUT_DIR, chunks[len(chunks) - 1].replace('.nt', ''), counter), 'w')
        counter += 1
        # Vertexes
        vertex_dict = {}
        #print offset, limit
        for key, data in graph_table.scan(filter="SingleColumnValueFilter('cf', 'id', >=, 'binary:%s', true, false) AND SingleColumnValueFilter('cf', 'id', <, 'binary:%s', true, false)" % (escape(struct.pack(">q", offset)), escape(struct.pack(">q", limit)))):
            vertex_dict[struct.unpack(">q", data['cf:id'])[0]] = data['cf:label']
        ordered_dict = collections.OrderedDict(sorted(vertex_dict.items()))
        for key in ordered_dict:
            f.write('v %s %s\n' % (key, ordered_dict[key]))

        #Edges
        for key, data in graph_table.scan(filter="SingleColumnValueFilter('cf', 'source', <, 'binary:%s', true, false) AND SingleColumnValueFilter('cf', 'target', <, 'binary:%s', true, false)" % (escape(struct.pack(">q", limit)), escape(struct.pack(">q", limit)))):
            f.write('e %s %s %s\n' % (struct.unpack(">q", data['cf:source'])[0], struct.unpack(">q", data['cf:target'])[0], data['cf:label']))
        f.close()
        offset = limit
        limit += VERTEX_LIMIT

    connection.close()
