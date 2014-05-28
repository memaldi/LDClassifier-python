from celery import Celery
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from model import Task
from datetime import datetime
from model import Settings, Task
import urllib2
import urllib
import json

app = Celery('tasks', broker='redis://localhost:6379/0')

engine = create_engine('sqlite:///db.sqlite', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

def virtuoso_insert(triple_list, task, settings):
    try:
        query = 'INSERT DATA INTO <%s> {' % task.graph
        for triple in triple_list:
            query += '%s\n' % triple
        query += '}'
        # print query
        request = urllib2.Request('%s?%s' % (settings.virtuoso_endpoint, urllib.urlencode({'query': query})))
        response = urllib2.urlopen(request)
        if response.code != 200:
            raise Exception
    except Exception as e:
        raise

@app.task
def launch_task(task_id):
    settings = session.query(Settings).first()
    task = session.query(Task).get(task_id)
    task.status = 'RUNNING'
    session.commit()
    end = False
    paused = False
    offset = task.offset
    while not end and not paused:
        triple_list = []
        query = 'SELECT DISTINCT * WHERE {?s ?p ?o} LIMIT 10 OFFSET %s' % offset
        print query
        # print query
        # print '%s?query=%s&output=json&format=json' % (task.endpoint, query)
        request = urllib2.Request('%s?query=%s&output=json&format=json' % (task.endpoint, query))
        try:
            response = urllib2.urlopen(request)
            if response.code == 200:
                data = response.read()
                json_data = json.loads(data)
                if len(json_data['results']['bindings']) > 0:
                    for json_triple in json_data['results']['bindings']:
                        subject = json_triple['s']['value']
                        predicate = json_triple['p']['value']
                        if json_triple['o']['type'] == "literal":
                            object = '"%s"' % json_triple['o']['value']
                        else:
                            object =  '<%s>' % json_triple['o']['value']
                        output_str = '<%s> <%s> %s .' % (subject, predicate, object)
                        triple_list.append(output_str)
                else:
                    end = True
            else:
                print response.read()
                task.paused_since = datetime.now()
                task.offset = offset
                task.status = 'PAUSED'
                paused = True
                session.commit()

        except Exception as e:
            print e
            print request.data
            print request.host
            task.paused_since = datetime.now()
            task.offset = offset
            task.status = 'PAUSED'
            paused = True
            session.commit()
        if not paused:
            try:
                virtuoso_insert(triple_list, task, settings)
                offset += 1000
            except:
                task.paused_since = datetime.now()
                task.offset = offset
                task.status = 'PAUSED'
                paused = True
                session.commit()