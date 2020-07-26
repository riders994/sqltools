from sqlalchemy import text, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import url, create_engine
from contextlib import contextmanager
import logging


DISPLAY_NAME = 'common.db'
TOOL_NAME = 'rvsqltools.' + DISPLAY_NAME
_logger = logging.getLogger(TOOL_NAME)


ENGINES_CACHE = {}


SEARCH_PATH_QUERY = "SET search_path TO {schema_name}"


def get_connection_kwargs(database, props):
    drivername = props['{db_name}.db_type'.format(db_name=database)]
    if drivername == 'postgresql':
        drivername += '+pg8000'
    kwargs = {
        'username': props['{db_name}.username'.format(db_name=database)],
        'password': props['{db_name}.password'.format(db_name=database)] or '',
        'host': props['{db_name}.host'.format(db_name=database)],
        'port': props['{db_name}.port'.format(db_name=database)],
        'database': props['{db_name}.database'.format(db_name=database)],
        'drivername': drivername
    }

    charset = props.get('{db_name}.charset'.format(db_name=database))

    if charset:
        kwargs['query'] = {'charset': charset}

    return kwargs


def build_connection_url(database, props):
    return url.URL(**get_connection_kwargs(database, props))


def build_connection(database, props, server_side_cursor=False):
    drivername = props['{db_name}.db_type'.format(db_name=database)]
    schema_name = props.get('{db_name}.schema'.format(db_name=database))

    engine_args = {'encoding': 'utf-8', 'pool_size': 20}

    if ENGINES_CACHE.get(database):
        _logger.info('Using existing connection to %s', database)
        conn = ENGINES_CACHE[database].connect()
        if drivername == 'postgresql':
            meta = MetaData(bind=conn, reflect=True, schema=schema_name)
            conn.execute(SEARCH_PATH_QUERY.format(schema_name=schema_name))

        return conn

    _logger.info('Opening connection to %s', database)
    db_url = build_connection_url(database, props)

    engine = create_engine(
        db_url, connect_args={}
    )
    ENGINES_CACHE[database] = engine

    conn = engine.connect()

    if drivername == 'postgresql':
        meta = MetaData(bind=conn, reflect=True, schema=schema_name)
        conn.execute(SEARCH_PATH_QUERY.format(schema_name=schema_name))

    return conn


class open_connection:
    """
    Opens a db connection to be used with a with statement
    """

    def __init__(self, databse, props, server_side_cursor=False):
        self.database, self.props, self.server_side_cursor = databse, props, server_side_cursor

    def __enter__(self):
        self.conn = build_connection(self.database, self.props, self.server_side_cursor)
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()


def get_session(orm_engine):
    session_cls = sessionmaker(bind=orm_engine)
    return session_cls()


def get_orm_engine(database, props):
    engine = create_engine(build_connection_url(database, props), **{})

    return engine


def get_orm_session(database, props):
    return get_session(orm_engine=get_orm_engine(database, props))


@contextmanager
def session_scope(orm_engine):
    session = get_session(orm_engine)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
