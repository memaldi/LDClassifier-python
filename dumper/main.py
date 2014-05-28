from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from model import Settings, Base, Task
from tasks import launch_task
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
    session.add(task)
    session.commit()
    print 'Launching task...'
    launch_task.delay(task.id)

def clear_tasks():
    tasks = session.query(Task).delete()

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
        print 'c) Exit'
        option = raw_input('Select your choice: ')

        if option == 'a':
            create_task()
        elif option == 'b':
            clear_tasks()
        elif option == 'c':
            print 'Bye!'
            sys.exit(0)
        else:
            print 'Wrong option!'


if  __name__ == '__main__':
    wizard()