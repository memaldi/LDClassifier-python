from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from model import Settings, Base

def wizard():
    print 'Welcome to external SPARQL endpoint to Virtuoso dumper.'
    engine = create_engine('sqlite:///db.sqlite', echo=True)
    Session = sessionmaker(bind=engine)
    session = Session()

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



if  __name__ == '__main__':
    wizard()