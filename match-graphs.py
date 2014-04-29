import argparse
import happybase
import struct
import uuid
import sys
from os import listdir
from os.path import isfile, join

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
    connection.create_table(table_name, {'graph': dict(), 'vertex': dict(), 'edge':dict()})

def load(args):
    connection = happybase.Connection(args.hbase_host, port=args.hbase_port, table_prefix=args.prefix)
    subgraphs = [ f for f in listdir(args.graph_dir) if isfile(join(args.graph_dir,f)) and f.endswith('.g') ]
    total = len(subgraphs)
    count = 1
    for subgraph in subgraphs:
        sys.stdout.write("\rLoading subgraphs (%s/%s)..." % (count, total))
        sys.stdout.flush()
        create_table(subgraph, connection, rewrite=True)
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

subparsers = parser.add_subparsers(help='action to perform', dest='command')

parser_load = subparsers.add_parser('load', help='load subgraphs into database')
parser_load.add_argument('graph_dir', help='directory containing substructures extracted by SUBDUE')
parser_reset = subparsers.add_parser('reset', help='drops all tables from the database')

args = parser.parse_args()

func = globals()[args.command]
func(args)