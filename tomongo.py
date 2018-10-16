#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
from optparse import OptionParser
import json
import namedtupled
import operator
import os
import pymysql.cursors
import pymongo
import sys
import time
import traceback
import yaml

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

this_cwd = os.path.dirname(os.path.realpath(__file__))
this_version = 1.0
this_log_prefix = 'mongo-to-mysql'

def date(unixtime, format = '%m/%d/%Y %H:%M:%S'):
    d = datetime.fromtimestamp(unixtime)
    return d.strftime(format)

def say(prefix, *msgs):
    s = ''

    if not msgs:
        return

    for msg in msgs:
        s += str(msg)

    out = "[%s] %s: %s" % (date(time.time()), prefix, s)

    print out

def connect_mysql(config, db):
    return pymysql.connect(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        passwd=config['passwd'],
        db=db,
        charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

def last_file_pos(conlogdb):
    sql = ("SELECT log_file, log_pos FROM clickhouse_changelog "
        "ORDER BY log_file DESC, log_pos DESC LIMIT 1")
        
    with conlogdb.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchone()

def master_status(conlogdb):
    sql = "SHOW MASTER STATUS"

    with conlogdb.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchone()

def init_params_command():
    p_usage = "Usage: %prog [options] COMMAND"
    p_desc = "Replicate tables from MySQL to MongoDB"
    p_epilog = ''

    parser = CustomOptionParser(p_usage, version="%prog " + str(this_version),
        description=p_desc, epilog=p_epilog)
    parser.add_option('-d', '--database', dest='database', type='string',
        help='Source MySQL database to replicate')
    parser.add_option('-t', '--table', dest='table', type='string',
        help='Source MySQL table to replicate')
    parser.add_option('-H', '--mysql-host', dest='mysql_host', type='string',
        help='Source MySQL host to replicate from')
    parser.add_option('-u', '--mysql-user', dest='mysql_user', type='string',
        help='Source MySQL user to login, must have REPLICATION SLAVE privilege')
    parser.add_option('-p', '--mysql-pass', dest='mysql_pass', type='string',
        help='Source MySQL connection password')
    parser.add_option('-P', '--mysql-port', dest='mysql_port', type='int',
        help='Source MySQL connection port')
    parser.add_option('-k', '--mysql-pk', dest='mysql_pk', type='string',
        help='Source MySQL table PRIMARY KEY column to be used for UPDATE/DELETE')

    parser.add_option('-D', '--db', dest='db', type='string',
        help='Destination MongoDB database to write to')
    parser.add_option('-c', '--collection', dest='collection', type='string',
        help='Destination MongoDB collection to write to')
    parser.add_option('-J', '--mongo-host', dest='mongo_host', type='string',
        help='Destination MongoDB host to write to')
    parser.add_option('-m', '--mongo-user', dest='mongo_user', type='string',
        help='Destination MongoDB user to login, must have REPLICATION SLAVE privilege')
    parser.add_option('-n', '--mongo-pass', dest='mongo_pass', type='string',
        help='Destination MongoDB connection password')
    parser.add_option('-o', '--mongo-port', dest='mongo_port', type='int',
        help='Destination MongoDB connection port')
    (options, args) = parser.parse_args()

    return options

def main():
    mysql_conf_source = {
        "host": this_options.mysql_host,
        "port": this_options.mysql_port,
        "user": this_options.mysql_user,
        "passwd": this_options.mysql_pass
    }

    values = None
    consrcdb = connect_mysql(mysql_conf_source, this_options.database)

    #file_pos = last_file_pos(conlogdb)
    #if file_pos is not None:
    #    log_file = file_pos['log_file']
    #    log_pos = file_pos['log_pos']
    #else:
    #    file_pos = master_status(consrcdb)
    #    log_file = file_pos['File']
    #    log_pos = file_pos['Position']
    file_pos = master_status(consrcdb)
    log_file = file_pos['File']
    log_pos = file_pos['Position']

    print "Starting streaming at file: %s, position: %s" % (log_file, log_pos)

    stream = BinLogStreamReader(
        connection_settings=mysql_conf_source, resume_stream=True,
        server_id=172313514, log_file=log_file, log_pos=log_pos,
        only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent], 
        blocking=True, only_schemas=this_options.database)

    mongo = pymongo.MongoClient(this_options.mongo_host, this_options.mongo_port)
    mongodb = mongo[this_options.db][this_options.collection]

    for binlogevent in stream:
        for row in binlogevent.rows:
            if binlogevent.table != this_options.table: continue

            try:
                if isinstance(binlogevent, DeleteRowsEvent):
                    values = row["values"]
                    res = mongodb.delete_one({ this_options.mysql_pk: values[this_options.mysql_pk] })
                    say(this_log_prefix, 'DELETE %s: %s' % (this_options.mysql_pk, values[this_options.mysql_pk]))
                elif isinstance(binlogevent, UpdateRowsEvent):
                    values = row["after_values"]
                    res = mongodb.replace_one({ this_options.mysql_pk: values[this_options.mysql_pk] }, values, upsert=True)
                    say(this_log_prefix, 'UPDATE %s: %s' % (this_options.mysql_pk, values[this_options.mysql_pk]))
                if isinstance(binlogevent, WriteRowsEvent):
                    values = row["values"]
                    res = mongodb.insert_one(values)
                    say(this_log_prefix, 'INSERT _id %s' % res.inserted_id)
                else:
                    continue

            except AttributeError, e:
                say(this_log_prefix, str(e))
                event = (binlogevent.schema, binlogevent.table,
                    stream.log_file, int(stream.log_pos))
                say(this_log_prefix, "Failed on: %s" % str(event))
                return 1

            sys.stdout.flush()

    stream.close()

# http://stackoverflow.com/questions/1857346/\
# python-optparse-how-to-include-additional-info-in-usage-output
class CustomOptionParser(OptionParser):
    def format_epilog(self, formatter):
        return self.epilog


if __name__ == "__main__":
    try:
        this_options = init_params_command()
        sys.exit(main())
    except Exception, e:
        say(this_log_prefix, this_log_prefix, 'An error has occurred. [%s]' % e)
        tb = traceback.format_exc().splitlines()
        for l in tb:
            say(this_log_prefix, l)
    finally:
        say(this_log_prefix, 'Done')
