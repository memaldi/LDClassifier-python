import argparse
import happybase
import struct
import uuid
import sys
import redis
import requests
from rdflib import Graph
from os import listdir
from os.path import isfile, join

ACCEPT_LIST = ['application/rdf+xml', 'text/n3', 'text/plain']
RDFLIB_CORRESPONDENCE = {'application/rdf+xml': 'xml', 'text/n3': 'n3', 'text/plain': 'nt'}
TMP_DIR = '/tmp'

def get_namespace(url):
    if '#' in url:
        return url.split('#')[0]
    else:
        surl = url.split('/')
        new_url = ''
        for chunk in surl[:len(surl) - 1]:
            new_url += chunk + '/'
        return new_url

def get_ontology(r, label):
    label = label.replace('<', '').replace('>', '').replace('"', '')
    ontology_file = r.get('%s:file' % label)
    ontology_serialization = None
    namespace = get_namespace(label)
    if ontology_file != None:
        ontology_serialization = r.get('%s:serialization' % label)
    else:
        for accept in ACCEPT_LIST:
            headers = {'Accept': accept}
            try:
                request = requests.get(namespace, headers=headers)
                if request.headers['content-type'] == accept:
                    ontology_serialization = accept
                    break
            except:
                pass
        if ontology_serialization in RDFLIB_CORRESPONDENCE:
            try:
                ontology_file = '%s/%s' % (TMP_DIR, str(uuid.uuid4()))
                ontology_serialization = RDFLIB_CORRESPONDENCE[ontology_serialization]
                f = open(ontology_file, 'w')
                f.write(request.text)
                f.close()
                r.set('%s:file' % namespace, ontology_file)
                r.set('%s:serialization' % namespace, ontology_serialization)
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


def generate_alignment(args):
    alignment_connection = happybase.Connection(args.hbase_host, port=args.hbase_port)
    #alignment_connection.create_table('alignments', {'cf': dict()})
    alignment_table = alignment_connection.table('alignments')

    connection = happybase.Connection(args.hbase_host, port=args.hbase_port, table_prefix=args.prefix)
    tables = connection.tables()

    r = redis.StrictRedis(host=args.redis_host, port=args.redis_port, db=args.redis_db)
    total = len(tables)
    count = 1
    for table_name in tables:
        sys.stdout.write("\rAnalyzing subgraphs (%s/%s)..." % (count, total))
        sys.stdout.flush()
        count += 1
        table = connection.table(table_name)
        for key, data in table.scan():
            label = ''
            if 'vertex:label' in data:
                label = data['vertex:label']
            else:
                label = data['edge:label']
            source_ontology_file, source_serialization = get_ontology(r, label)
            if source_ontology_file != None and source_serialization != None:
                target_ontology_file, target_serialization = get_ontology(r, label)
                if target_ontology_file != None and target_serialization != None:
                        source_ontology = parse_ontology(source_ontology_file, source_serialization)
                        target_ontology = parse_ontology(target_ontology_file, target_serialization)


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
parser_load.add_argument('rewrite', help='if a graph already exists, is replaced in database', default=False, type=bool)

parser_reset = subparsers.add_parser('reset', help='drops all tables from the database')

parser_generate_alignment = subparsers.add_parser('generate_alignment', help='generate alignments among classes and properties found in substructures')

args = parser.parse_args()

func = globals()[args.command]
func(args)