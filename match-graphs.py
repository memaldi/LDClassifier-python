import argparse
import happybase
import struct
import uuid
import sys
import stringdistances
from os import listdir
from os.path import isfile, join

ACCEPT_LIST = ['application/rdf+xml', 'text/n3', 'text/plain', 'application/owl+xml']
RDFLIB_CORRESPONDENCE = {'application/rdf+xml': 'xml', 'text/n3': 'n3', 'text/plain': 'nt', 'application/owl+xml': 'xml'}
BLACKLIST = []
MATCHING_FUNCTIONS = ['substring_distance', 'equal_distance', 'levenshtein_distance', 'smoa_distance']

def get_namespace(url):
    url = url.replace('<', '').replace('>', '').replace('"', '')
    if '#' in url:
        return url.split('#')[0]
    else:
        surl = url.split('/')
        new_url = ''
        for chunk in surl[:len(surl) - 1]:
            new_url += chunk + '/'
        return new_url

def get_entity_name(url):
    if '#' in url:
        return url.split('#')[1]
    else:
        if url[len(url) - 1] == '/':
            url = url[:len(url) - 1]
        surl = url.split('/')
        return surl[len(surl) - 1]

def distance(source, target, function):
    source_name = get_entity_name(source)
    target_name = get_entity_name(target)
    method = getattr(stringdistances, function)
    return method(source_name, target_name)

def clean_label(label):
    label = label.replace('<', '').replace('>', '').replace('"', '')
    return label

def generate_alignment(args):
    alignment_connection = happybase.Connection(args.hbase_host, port=args.hbase_port)
    try:
        alignment_connection.create_table('alignments', {'cf': dict()})
    except:
        pass
    alignment_table = alignment_connection.table('alignments')

    connection = happybase.Connection(args.hbase_host, port=args.hbase_port, table_prefix=args.prefix)
    tables = connection.tables()

    # Get all Classes and properties
    total = len(tables)
    count = 1
    for table_name in tables:
        sys.stdout.write("\rAnalyzing classes and properties from subgraphs (%s/%s)..." % (count, total))
        sys.stdout.flush()
        count += 1
        table = connection.table(table_name)
        for key, data in table.scan():
            label = ''
            if 'vertex:label' in data:
                label = data['vertex:label']
                # Store Class
                row = table.row(label)
                if not row:
                    alignment_table.put(label, {'cf:type': 'Class'})
            else:
                label = data['edge:label']
                # Store property
                row = table.row(label)
                if not row:
                    alignment_table.put(label, {'cf:type': 'property'})

    print ''

    # Match Classes and properties
    # Match Classes
    print 'Matching classes...'
    for source_key, source_data in alignment_table.scan(filter="SingleColumnValueFilter ('cf', 'type', =, 'binary:Class', true, true)"):
        for target_key, target_data in alignment_table.scan(filter="SingleColumnValueFilter ('cf', 'type', =, 'binary:Class', true, true)"):
            if source_key != target_key and get_namespace(source_key) != get_namespace(target_key):
                for function in MATCHING_FUNCTIONS:
                    found = False
                    for key, data in alignment_table.scan(filter="SingleColumnValueFilter ('cf', 'source', =, 'binary:%s') AND SingleColumnValueFilter('cf', 'target', =, 'binary:%s') AND SingleColumnValueFilter('cf', 'function', =, 'binary:%s', true, false)" % (source_key, target_key, function)):
                        #print key, data
                        found = True
                    if not found:
                        dist = distance(clean_label(source_key), clean_label(target_key), function)
                        alignment_table.put(str(uuid.uuid4()), {'cf:source': source_key, 'cf:target': target_key, 'cf:function': function, 'cf:dist': struct.pack(">f", dist)})
                        #print source_key, target_key, function, dist
    # Match properties
    print 'Matching properties...'
    for source_key, source_data in alignment_table.scan(filter="SingleColumnValueFilter ('cf', 'type', =, 'binary:property', true, true)"):
        for target_key, target_data in alignment_table.scan(filter="SingleColumnValueFilter ('cf', 'type', =, 'binary:property', true, true)"):
            if source_key != target_key and get_namespace(source_key) != get_namespace(target_key):
                for function in MATCHING_FUNCTIONS:
                    found = False
                    for key, data in alignment_table.scan(filter="SingleColumnValueFilter ('cf', 'source', =, 'binary:%s') AND SingleColumnValueFilter('cf', 'target', =, 'binary:%s') AND SingleColumnValueFilter('cf', 'function', =, 'binary:%s', true, false)" % (source_key, target_key, function)):
                        found = True
                    if not found:
                        dist = distance(clean_label(source_key), clean_label(target_key), function)
                        alignment_table.put(str(uuid.uuid4()), {'cf:source': source_key, 'cf:target': target_key, 'cf:function': function, 'cf:dist': struct.pack(">f", dist)})
                        #print source_key, target_key, function, dist

    connection.close()

def reset(args):
    connection = happybase.Connection(args.hbase_host, port=args.hbase_port, table_prefix=args.prefix)
    tables = connection.tables()
    total = len(tables)
    count = 1
    for table in tables:
        sys.stdout.write("\rDeleting tables (%s/%s)..." % (count, total))
        sys.stdout.flush()
        connection.disable_table(table)
        connection.delete_table(table)
        count += 1
    connection.close()
    print ''
    print 'Done!'

def create_table(table_name, connection, rewrite=False):
    if rewrite:
        try:
            connection.disable_table(table_name)
            connection.delete_table(table_name)
        except:
            pass
    try:
        connection.create_table(table_name, {'graph': dict(), 'vertex': dict(), 'edge':dict()})
    except:
        pass

def load(args):
    connection = happybase.Connection(args.hbase_host, port=args.hbase_port, table_prefix=args.prefix)
    subgraphs = [ f for f in listdir(args.graph_dir) if isfile(join(args.graph_dir,f)) and f.endswith('.g') ]
    total = len(subgraphs)
    count = 1
    for subgraph in subgraphs:
        sys.stdout.write("\rLoading subgraphs (%s/%s)..." % (count, total))
        sys.stdout.flush()
        create_table(subgraph, connection, rewrite=args.rewrite)
        table = connection.table(subgraph)
        with open('%s/%s' % (args.graph_dir, subgraph)) as sf:
            for line in sf:
                sl = line.replace('\n', '').split(' ')
                if line.startswith('v'):
                    table.put(struct.pack(">q", int(sl[1])), {'graph:type': 'v', 'vertex:label': sl[2]})
                elif line.startswith('d'):
                    key = uuid.uuid4()
                    table.put(str(key), {'graph:type': 'e', 'edge:source': struct.pack(">q", int(sl[1])), 'edge:target': struct.pack(">q", int(sl[2])), 'edge:label': sl[3]})
        count += 1
    connection.close()
    print ''
    print 'Done!'

def match_edges(source_table_name, target_table_name, connection, alignment_connection, matching_threshold):
    source_labels = set()
    target_labels = set()
    print source_table_name, target_table_name
    source_table = connection.table(source_table_name)
    target_table = connection.table(target_table_name)
    alignment_table = alignment_connection.table('alignments')
    for source_key, source_data in source_table.scan(filter="SingleColumnValueFilter ('graph', 'type', =, 'binary:e', true, false)"):
        source_labels.add(source_data['edge:label'])
    for target_key, target_data in target_table.scan(filter="SingleColumnValueFilter ('graph', 'type', =, 'binary:e', true, false)"):
        target_labels.add(target_data['edge:label'])
    sim_dict = {}
    for source_label in source_labels:
        for target_label in target_labels:
            if source_label != target_label:
                similarity = 0
                func_count = 0
                dist = 0
                for key, data in alignment_table.scan(filter="SingleColumnValueFilter ('cf', 'source', =, 'binary:%s', true, false) AND SingleColumnValueFilter ('cf', 'target', =, 'binary:%s', true, false)" % (source_label, target_label)):
                    func_count += 1
                    dist += struct.unpack(">f", data['cf:dist'])[0]
                if dist > 0:
                    similarity = 1 - float(dist / func_count)
                if similarity > matching_threshold:
                    if source_label not in sim_dict:
                        sim_dict[source_label] = {}
                    sim_dict[source_label][target_label] = similarity

            else:
                if source_label not in sim_dict:
                    sim_dict[source_label] = {}
                sim_dict[source_label][target_label] = 1
                break
    vertex_replace_dict = {}
    target_dict = {}
    for key in sim_dict:
        max_sim = 0
        max_sim_label = None
        for key2 in sim_dict[key]:
            if sim_dict[key][key2] > max_sim:
                max_sim = sim_dict[key][key2]
                max_sim_label = key2

        if max_sim_label not in target_dict:
            target_dict[max_sim_label] = 0
        if max_sim > target_dict[max_sim_label]:
            target_dict[max_sim_label] = max_sim
            vertex_replace_dict[key] = max_sim_label
            vertex_replace_dict[max_sim_label] = key
    return vertex_replace_dict

def match_vertexes(source_table_name, target_table_name, connection, alignment_connection, matching_threshold):
    source_labels = set()
    target_labels = set()
    print source_table_name, target_table_name
    source_table = connection.table(source_table_name)
    target_table = connection.table(target_table_name)
    alignment_table = alignment_connection.table('alignments')
    for source_key, source_data in source_table.scan(filter="SingleColumnValueFilter ('graph', 'type', =, 'binary:v', true, false)"):
        source_labels.add(source_data['vertex:label'])
    for target_key, target_data in target_table.scan(filter="SingleColumnValueFilter ('graph', 'type', =, 'binary:v', true, false)"):
        target_labels.add(target_data['vertex:label'])

    sim_dict = {}
    for source_label in source_labels:
        for target_label in target_labels:
            if source_label != target_label:
                similarity = 0
                func_count = 0
                dist = 0
                for key, data in alignment_table.scan(filter="SingleColumnValueFilter ('cf', 'source', =, 'binary:%s', true, false) AND SingleColumnValueFilter ('cf', 'target', =, 'binary:%s', true, false)" % (source_label, target_label)):
                    func_count += 1
                    dist += struct.unpack(">f", data['cf:dist'])[0]
                if dist > 0:
                    similarity = 1 - float(dist / func_count)
                if similarity > matching_threshold:
                    if source_label not in sim_dict:
                        sim_dict[source_label] = {}
                    sim_dict[source_label][target_label] = similarity

            else:
                if source_label not in sim_dict:
                    sim_dict[source_label] = {}
                sim_dict[source_label][target_label] = 1
                break
    vertex_replace_dict = {}
    target_dict = {}
    for key in sim_dict:
        max_sim = 0
        max_sim_label = None
        for key2 in sim_dict[key]:
            if sim_dict[key][key2] > max_sim:
                max_sim = sim_dict[key][key2]
                max_sim_label = key2

        if max_sim_label not in target_dict:
            target_dict[max_sim_label] = 0
        if max_sim > target_dict[max_sim_label]:
            target_dict[max_sim_label] = max_sim
            vertex_replace_dict[key] = max_sim_label
            vertex_replace_dict[max_sim_label] = key
    return vertex_replace_dict

def sim(args):
    connection = happybase.Connection(args.hbase_host, port=args.hbase_port, table_prefix=args.prefix)
    alignment_connection = happybase.Connection(args.hbase_host, port=args.hbase_port)
    #alignment_table = alignment_connection.table('alignments')
    tables = connection.tables()

    total = len(tables)
    count = 1
    for source_table_name in tables:
        sys.stdout.write("\rMatching subgraphs (%s/%s)..." % (count, total))
        sys.stdout.flush()
        count += 1
        #source_table = connection.table(source_table_name)
        for target_table_name in tables:
            if source_table_name != target_table_name:
                #target_table = connection.table(target_table_name)
                # Vertexes
                vertex_replace_dict = match_vertexes(source_table_name, target_table_name, connection, alignment_connection, args.matching_threshold)
                # Edges
                edge_replace_dict = match_edges(source_table_name, target_table_name, connection, alignment_connection, args.matching_threshold)


    print ''

parser = argparse.ArgumentParser(description='Match substructures.')
parser.add_argument('-prefix', help='prefix for tables. Default: graph', default='graph')
parser.add_argument('-hbase_host', help='HBase host address. Default: localhost', default='localhost')
parser.add_argument('-hbase_port', help='HBase connection port. Default: 9090', default=9090, type=int)
parser.add_argument('-redis_host', help='Redis host address. Default: localhost', default='localhost')
parser.add_argument('-redis_port', help='Redis connection port. Default: 6379', default=6379, type=int)
parser.add_argument('-redis_db', help='Redis db id. Default: 0', default=0, type=int)

subparsers = parser.add_subparsers(help='action to perform', dest='command')

parser_load = subparsers.add_parser('load', help='load subgraphs into database')
parser_load.add_argument('graph_dir', help='directory containing substructures extracted by SUBDUE')
parser_load.add_argument('-rewrite', help='if a graph already exists, is replaced in database. Default: False', default=False, type=bool)

parser_reset = subparsers.add_parser('reset', help='drops all tables from the database')

parser_generate_alignment = subparsers.add_parser('generate_alignment', help='generate alignments among classes and properties found in substructures')

parser_similarities = subparsers.add_parser('sim', help='generate similarities among datasets')
parser_similarities.add_argument('matching_threshold', help='value over is considered that two entities represent the same concept', type=float)
parser_similarities.add_argument('-tmp_dir', help='dir in which SUBDUE input files are stored. Default: /tmp', default='/tmp')

args = parser.parse_args()

func = globals()[args.command]
func(args)