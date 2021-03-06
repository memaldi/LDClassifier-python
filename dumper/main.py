from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from model import Settings, Base, Task
from tasks import launch_task
from datetime import datetime
import sys

engine = create_engine('sqlite:///db.sqlite', echo=True)
Session = sessionmaker(bind=engine)
session = Session()

def create_task():
    print 'Creating a new task:'
    sparql_endpoint = raw_input('Source SPARQL endpoint: ')
    graph = raw_input('Named graph from Virtuoso in which data is going to be stored: ')
    task = Task()
    task.endpoint = sparql_endpoint
    task.graph = graph
    task.offset = 0
    task.start_time = datetime.now()
    session.add(task)
    session.commit()
    print 'Launching task...'
    launch_task.delay(task.id)

def clear_tasks():
    tasks = session.query(Task).delete()
    session.commit()

def show_tasks():
    tasks = session.query(Task)

    print 'id | SPARQL endpoint | Named graph | Status | Start time | End time | Paused since | Offset'
    for task in tasks:
        print '%s | %s | %s | %s | %s | %s | %s | %s' % (task.id, task.endpoint, task.graph, task.status, task.start_time, task.end_time, task.paused_since, task.offset)

def resume_task():
    id = raw_input('Task id: ')
    task = session.query(Task).get(id)
    if task.status != 'PAUSED':
        print 'This task is not paused!'
    else:
        print 'Launching task...'
        launch_task.delay(id)

def wizard():
    print 'Welcome to external SPARQL endpoint to Virtuoso dumper.'

    if not engine.dialect.has_table(engine.connect(), 'task'):
        Base.metadata.create_all(engine)

    settings = session.query(Settings).first()

    if settings == None:
        print 'It seems that is the first time that you use this tool. Pleas fulfill the following config parameters:'
        virtuoso_endpoint = raw_input('Virtuoso SPARQL endpoint: ')
        settings = Settings()
        settings.virtuoso_endpoint = virtuoso_endpoint
        session.add(settings)
        session.commit()
        print ''

    exit = False

    while not exit:
        print 'a) Create a new dump task'
        print 'b) Delete all tasks'
        print 'c) Show tasks'
        print 'd) Resume task'
        print 'h) Exit'
        option = raw_input('Select your choice: ')

        if option == 'a':
            create_task()
        elif option == 'b':
            clear_tasks()
        elif option == 'c':
            show_tasks()
        elif option == 'd':
            resume_task()
        elif option == 'h':
            print 'Bye!'
            sys.exit(0)
        else:
            print 'Wrong option!'


if  __name__ == '__main__':
    wizard()