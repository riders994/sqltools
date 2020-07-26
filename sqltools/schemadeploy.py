import os
import inspect
from collections import defaultdict
from sqlalchemy import create_engine
from sqltools.common.db import build_connection, build_connection_url

import logging

DISPLAY_NAME = 'deployer'
TOOL_NAME = 'rvsqltools.' + DISPLAY_NAME
_logger = logging.getLogger(TOOL_NAME)

MANIFEST = ''


class AbstractDBTracker:
    CREATE_DEPLOYMENT_TRACKER_SQL = ''
    DEPLOYMENT_TRACKER_EXISTS_SQL = ''
    UPDATE_DEPLOYMENT_TRACKER_SQL = ''
    GET_DEPLOYMENT_TRACKER_RECORD_SQL = ''
    DELETE_DEPLOYMENT_TRACKER_RECORD_SQL = ''
    VERIFY_DEPLOYMENT_TRACKER_RECORD_SQL = ''
    ADD_TO_TRACKER_SQL = ''

    def __init__(self, db_conn, schema_path):
        self.db_conn = db_conn
        self.schema_path = schema_path

        if not self._deployment_tracker_exists():
            self._create_deployment_tracker()

    def _deployment_tracker_exists(self):
        return self.db_conn.execute(self.DEPLOYMENT_TRACKER_EXISTS_SQL).fetchone()[0]

    def _create_deployment_tracker(self):
        self.db_conn.execute(self.CREATE_DEPLOYMENT_TRACKER_SQL)

    def get_executed_scripts(self):
        executed = self.db_conn.execute(self.GET_DEPLOYMENT_TRACKER_RECORD_SQL).fetchall()
        return set(x[0] for x in executed)

    def mark_script_as_run(self, script_name):
        self.db_conn.execute(self.UPDATE_DEPLOYMENT_TRACKER_SQL, (script_name,))

    def unmark_script_as_run(self, script_name):
        self.db_conn.execute(self.DELETE_DEPLOYMENT_TRACKER_RECORD_SQL, (script_name,))

    def add_script_to_tracker(self, script_name, script):
        self.db_conn.execute(self.ADD_TO_TRACKER_SQL, (script_name, script,))

    def verify_script_exists(self, script_name):
        return bool(self.db_conn.execute(self.VERIFY_DEPLOYMENT_TRACKER_RECORD_SQL, (script_name,)).fetchone()[0])


class PostGRESTracker(AbstractDBTracker):
    CREATE_DEPLOYMENT_TRACKER_SQL = ''
    DEPLOYMENT_TRACKER_EXISTS_SQL = ''
    UPDATE_DEPLOYMENT_TRACKER_SQL = ''
    GET_DEPLOYMENT_TRACKER_RECORD_SQL = ''
    DELETE_DEPLOYMENT_TRACKER_RECORD_SQL = ''
    VERIFY_DEPLOYMENT_TRACKER_RECORD_SQL = ''
    ADD_TO_TRACKER_SQL = ''

    def __init__(self, db_conn, schema_path):
        super(PostGRESTracker, self).__init__(db_conn, schema_path)


class SchemaDeployer:
    def __init__(self, props, dry_run=False, verbose=False):
        self.props = props
        self.dry_run = dry_run
        self.verbose = verbose
        self.schemas_path = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
        self.manifest = MANIFEST

    def _execute_statements(self, db_conn, statements):
        transact = db_conn.begin()
        cur = ''
        l = len(statements)
        try:
            for i, sql in enumerate(statements):
                cur = sql
                if sql.strip() != '' and not sql.strip().startswith('#'):
                    if self.dry_run:
                        _logger.info('Pretending to execute %s', sql)
                    else:
                        if self.verbose:
                            _logger.info('Executing statement {i} out of {n}: \n\n{s}\n\n'.format(i=i, n=l, s=cur))
                        db_conn.execute(sql)
            transact.commit()
        except Exception as e:
            _logger.info('Failed on statment {i}: \n\n{s}'.format(i=i, s=cur))
            transact.rollback()
            return False
        return True

    def _create_tracker(self, db_conn, db_name):
        if self.props['{}.db_type'] == 'postgresql':
            tracker = PostGRESTracker(db_conn, self.schemas_path)
        else:
            raise NotImplementedError('Unsupported database engine: %s' % self.props['{}.db_type'.format(db_name)])
        return tracker

    def create_from_baseline(self, db_name):
        _logger.info('Creating {} database from baseline...'.format(db_name))
        db_conn = build_connection_url(database=db_name, props=self.props)
        baseline_dir = os.path.join(self.schemas_path, db_name, 'baseline')
        sql_paths = [
            os.path.join(baseline_dir, f) for f in os.listdir(baseline_dir) if f.endswith('.sql') or f.endswith('.proc')
        ]
        sql_paths.sort()

        for path in sql_paths:
            _logger.info('Executing %s', path)
            with open(path, 'r') as file:
                if path.endswith('.sql'):
                    statements = file.read().split(';')
                elif path.endswith('.proc'):
                    statements = [file.read()]

                self._execute_statements(db_conn, statements)

            connection_string = build_connection_url(database=db_name, props=self.props)
            db_eng = create_engine(connection_string)
            self._create_tracker(db_eng, db_name)

    def get_manifest(self):
        return self.manifest

    def mark_script_as_run(self, script):
        script = script.strip()
        db_name = os.path.dirname(script)
        if not db_name:
            _logger.error('No db name provided, cannot mark %s as run', script)
            return
        connection_string = build_connection_url(db_name, self.props)
        db_eng = create_engine(connection_string)
        tracker = self._create_tracker(db_eng, db_name)
        _logger.info('Marking script {} as run'.format(script))
        tracker.mark_script_as_run(script)

    def unmark_script_as_run(self, script):
        script = script.strip()
        db_name = os.path.dirname(script)
        if not db_name:
            _logger.error('No db name provided, cannot mark %s as run', script)
            return
        connection_string = build_connection_url(db_name, self.props)
        db_eng = create_engine(connection_string)
        tracker = self._create_tracker(db_eng, db_name)
        if not tracker.verify_script_exists(script):
            raise ValueError('Scipt {} not found to unmark'.format(script))
        _logger.info('Unmarking script {} as run'.format(script))

        tracker.unmark_script_as_run(script)

    def add_to_tracker(self, script):
        script = script.strip()
        name = os.path.basename(script).split('.')[0]
        db_name = os.path.dirname(script)
        if not db_name:
            _logger.error('No db name provided, cannot mark %s as run', script)
            return
        connection_string = build_connection_url(db_name, self.props)
        db_eng = create_engine(connection_string)
        tracker = self._create_tracker(db_eng, db_name)
        _logger.info('Marking script {} as run'.format(script))
        tracker.add_script_to_tracker(name, script)

    def execute_updates(self, databases=None, track_deployment=True):
        manifest = self.get_manifest()
        script_databases = defaultdict(dict)
        # iterate rows of manifest
        for row in manifest:
            database = row['db']
            script = row['name']
            if not database:
                continue
            if not databases or database in databases:
                script_databases[database].update({script: row['command']})

        executed_scripts = set()
        deployment_trackers = {}
        if track_deployment:
            for db_name in script_databases.keys():
                connection_str = build_connection_url(db_name, self.props)
                db_eng = create_engine(connection_str)
                tracker = self._create_tracker(db_eng, db_name)
                deployment_trackers[db_name] = tracker

            for deployment_tracker in deployment_trackers:
                executed_scripts = executed_scripts.union(
                    deployment_trackers[deployment_tracker].get_executed_scripts()
                )

        for database, scripts in script_databases.items():
            db_conn = build_connection(database, self.props)
            for name, script in scripts.items():
                if name not in executed_scripts:
                    _logger.info('Executing script: {}'.format(name))
                    with open(os.path.join(self.schemas_path), 'r') as file:
                        if script.endswith('.sql'):
                            statements = file.read().split(';')
                        elif script.endswith('.proc'):
                            statements = [file.read()]
                    success = self._execute_statements(db_conn, statements)
                    if success and track_deployment:
                        self.mark_script_as_run(name)


class DeployJob:
    def __init__(self):
        pass

    def run(self):
        _logger.info('Starting deployment...')

        deployer = SchemaDeployer(self.props, self.opts.dry_run, self.opts.verbose)
        if self.opts.mark_as_run:
            script_name = self.opts.mark_as_run
            deployer.mark_script_as_run(script_name)
        elif self.opts.unmark_as_run:
            script_name = self.opts.unmark_as_run
            deployer.unmark_script_as_run(script_name)
        elif self.opts.add_script:
            script_name = self.opts.add_script
            deployer.add_to_tracker(script_name)
        else:
            deployer.execute_updates(self.opts.update_database_filter)
        _logger.info('Database Deployment update complete.')

