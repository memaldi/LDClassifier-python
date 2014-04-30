import argparse
import happybase
import struct
import uuid
import sys
import redis
import requests
import stringdistances
from rdflib import Graph
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

def get_ontology(r, namespace, args):
    ontology_file = r.get('%s:%s:file' % (args.prefix, namespace))
    ontology_serialization = None
    if ontology_file != None:
        ontology_serialization = r.get('%s:%s:serialization' % (args.prefix, namespace))
    else:
        found = False
        for accept in ACCEPT_LIST:
            headers = {'Accept': accept}
            try:
                request = requests.get(namespace, headers=headers, timeout=10)
                if accept in request.headers['content-type']:
                    ontology_serialization = accept
                    found = True
                    break
            except Exception as e:
                BLACKLIST.append(namespace)
                break

        if not found and namespace not in BLACKLIST:
            try:
                request = requests.get(namespace, timeout=10)
                for accept in ACCEPT_LIST:
                    if accept in request:
                        ontology_serialization = accept
            except:
                pass

        if ontology_serialization in RDFLIB_CORRESPONDENCE:
            try:
                ontology_file = '%s/%s' % (args.ontology_dir, str(uuid.uuid4()))
                ontology_serialization = RDFLIB_CORRESPONDENCE[ontology_serialization]
                f = open(ontology_file, 'w')
                f.write(request.text)
                f.close()
                r.set('%s:%s:file' % (args.prefix, namespace), ontology_file)
                r.set('%s:%s:serialization' % (args.prefix, namespace), ontology_serialization)
            except:
                pass

    return ontology_file, ontology_serialization

def parse_ontology(ontology_file, serialization):
    ontology = Graph()
    try:
        ontology.parse(ontology_file, format=serialization)
        return ontology
    except:
        for key in RDFLIB_CORRESPONDENCE:
            try:
                ontology.parse(ontology_file, format=RDFLIB_CORRESPONDENCE[key])
                return ontology
            except:
                pass
    return None

def get_classes(ontology):
    class_list = []
    sparql_result = ontology.query('SELECT DISTINCT ?s WHERE {?s a owl:Class}')
    if len(sparql_result) == 0:
        sparql_result = ontology.query('SELECT DISTINCT ?s WHERE {?s a rdfs:Class}')
    for s in sparql_result:
        class_list.append(s)
    return class_list

def get_entity_name(url):
    if '#' in url:
        return url[1]
    else:
        if url[len(url) - 1] == '/':
            url = url[:len(url) - 1]
        surl = url.split('/')
        return surl[len(surl) - 1]

def distance(source, target, function):
    source_name = get_entity_name(source[0])
    target_name = get_entity_name(target[0])
    method = getattr(stringdistances, function)
    method(source_name, target_name)

def match_classes(source_ontology, target_ontology):
    source_classes = get_classes(source_ontology)
    target_classes = get_classes(target_ontology)
    total = len(source_classes)
    count = 1
    for source_class in source_classes:
        sys.stdout.write("\rMatching classes (%s/%s)..." % (count, total))
        sys.stdout.flush()
        count += 1
        for target_class in target_classes:
            for function in MATCHING_FUNCTIONS:
                dist = distance(source_class, target_class, function)
                print source_class, target_class, function, dist
    print ''

def generate_alignment(args):
    alignment_connection = happybase.Connection(args.hbase_host, port=args.hbase_port)
    #alignment_connection.create_table('alignments', {'cf': dict()})
    alignment_table = alignment_connection.table('alignments')

    connection = happybase.Connection(args.hbase_host, port=args.hbase_port, table_prefix=args.prefix)
    tables = connection.tables()

    # Get all ontologies
    r = redis.StrictRedis(host=args.redis_host, port=args.redis_port, db=args.redis_db)
    total = len(tables)
    count = 1
    for table_name in tables:
        sys.stdout.write("\rAnalyzing ontologies from subgraphs (%s/%s)..." % (count, total))
        sys.stdout.flush()
        count += 1
        table = connection.table(table_name)
        for key, data in table.scan():
            label = ''
            if 'vertex:label' in data:
                label = data['vertex:label']
            else:
                label = data['edge:label']
            namespace = get_namespace(label)
            if not r.exists('%s:%s:file' % (args.prefix, namespace)) and namespace not in BLACKLIST:
                get_ontology(r, namespace, args)
    print ''

    # Match ontologies
    total = len(r.keys('%s:*:file' % args.prefix))
    count = 1
    for source_key in r.keys('%s:*:file' % args.prefix):
        sys.stdout.write("\rMatching ontologies (%s/%s)..." % (count, total))
        sys.stdout.flush()
        count += 1
        for target_key in r.keys('%s:*:file' % args.prefix):
            if source_key != target_key:
                source_ontology = parse_ontology(r.get(source_key), r.get(source_key.replace(':file', ':serialization')))
                if source_ontology != None:
                    target_ontology = parse_ontology(r.get(source_key), r.get(source_key.replace(':file', ':serialization')))
                    match_classes(source_ontology, target_ontology)
    print ''

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
parser_generate_alignment.add_argument('-ontology_dir', help='Directory where store ontologies', default='/tmp')

args = parser.parse_args()

func = globals()[args.command]
func(args)