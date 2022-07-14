#!usr/bin/env python3

import sqlite3
import csv
import json
from collections import namedtuple

Data = namedtuple(
    'Data',
    ['headers', 'rows']
)

Connection = namedtuple(
    'Connection',
    ['db', 'cur']
)


class SQLiteLoader:

    def __init__(self, infile, target_db, tablename):
        self.infile = infile
        self.db = target_db
        self.tablename = tablename
        self.__missing_db_error = "OH JEEZ: Seems like the target_db parameter isn't set correctly. Without it, we can't make a database table."
        self.__missing_db_params_error = "GOLLY: Not seeing either headers or a tablename present. Without those, we can't make a database table."

        if not self.tablename or not self.db:
            raise Exception(self.__missing_db_error)

    def prepare_csv_data(self, filename, delimiter=',', has_headers=False):
        with open(filename) as f:
            reader = csv.reader(f, delimiter=delimiter, quotechar='"')
            if has_headers:
                headers = tuple(next(reader))
            else:
                headers = tuple()

            return Data(
                headers,
                [tuple(row) for row in reader]
            )

    def prepare_json_data(self, filename):
        with open(filename) as f:
            j = json.loads(f)
            headers = list(j.keys())

            return Data(
                headers,
                [tuple(row) for row in j.values()]
            )

    def connect_to_db(self, target_db):
        db = sqlite3.connect(target_db)
        return Connection(
            db,
            db.cursor()
        )

    def load_data(self, data, headers, tablename, db_connection):
        try:
            db_connection.cur.execute(f'CREATE TABLE IF NOT EXISTS {tablename} {headers}')
        except sqlite3.OperationalError:
            print(self.infile, self.db, self.tablename)
            raise Exception(self.__missing_db_params_error)

        header_count = len(headers)
        sql = f'INSERT INTO {tablename} VALUES(' + ','.join(header_count * ['?']) + ')'
        db_connection.cur.executemany(sql, data)
        db_connection.db.commit()
        db_connection.db.close()

    def run_the_jewels(self, filename, target_db, tablename, has_headers, delimiter=','):
        if filename.endswith('.csv'):
            data = self.prepare_csv_data(filename, delimiter=delimiter, has_headers=has_headers)
        elif filename.endswith('.json'):
            data = self.prepare_json_data(filename)

        if data.headers and tablename:
            connection = self.connect_to_db(target_db)
            self.load_data(data.rows, data.headers, tablename, connection)
        else:
            raise Exception(self.__missing_db_params_error)


if __name__ == '__main__':
    filename = 'your_filepath_here'
    target_db = 'your_target_db_here'
    tablename = 'your_table_name_here'
    loader = SQLiteLoader(filename, target_db, tablename)
    loader.run_the_jewels(loader.infile, loader.db, loader.tablename, True)
