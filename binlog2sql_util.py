# -*- coding: utf-8 -*-
# matomo2clickhouse
# https://github.com/dneupokoev/matomo2clickhouse
#
# Replication Matomo from MySQL to ClickHouse
# Репликация Matomo: переливка данных из MySQL в ClickHouse
#
binlog2sql_util_version = '230510.01'
#
# 230510.01:
# + отключил get_correct_sql
#
# 230404.01:
# + добавил поля sql_4insert_table и sql_4insert_values - для объединения инсертов в один большой
#
# 230403.01:
# + добавил функцию get_schema_clickhouse и переписал все с учетом этой функции
#
# 221005.01:
# + базовая стабильная версия (полностью протестированная и отлаженная)
#


import settings
import os
import sys
import re
import copy
import argparse
import datetime
import time
import getpass
from contextlib import contextmanager
from pymysqlreplication.event import QueryEvent
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)

if sys.version > '3':
    PY3PLUS = True
else:
    PY3PLUS = False


def get_schema_clickhouse(in_schema=''):
    try:
        out_schema = settings.CH_matomo_dbname
    except:
        out_schema = in_schema
    return out_schema


def get_correct_sql(sql=''):
    # # sql = sql.replace('=NULL', ' is NULL')
    # #
    # sql = re.sub(r", '([-]{0,1}[0-9]{1,16}[.][0-9]{1,16}[e]{0,8}[0-9])'", r", \1", sql)
    # sql = re.sub(r", ([-]{0,1}[0-9]{1,16}[.][0-9]{1,16}[e]{0,8}[0-9])", r", '\1'", sql)
    # #
    # sql = re.sub(r"`='([-]{0,1}[0-9]{1,16}[.][0-9]{1,16}[e]{0,8}[0-9])'", r"`=\1", sql)
    # sql = re.sub(r"`=([-]{0,1}[0-9]{1,16}[.][0-9]{1,16}[e]{0,8}[0-9])", r"`='\1'", sql)
    # #
    # sql = re.sub(r"`='([-]{0,1}[0-9]{1,16}[.][0-9]{1,16}[e]{0,8}[0-9])' AND", r"`=\1 AND", sql)
    # sql = re.sub(r"`=([-]{0,1}[0-9]{1,16}[.][0-9]{1,16}[e]{0,8}[0-9]) AND", r"`='\1' AND", sql)
    # #
    return sql


def get_dateid():
    '''
    Функция возвращает псевдоуникальный (скорее всего именно уникальный, т.к. учитывает доли секунд) ключ, основанный на дате:
    UInt64
    Пример: 16647739613876690
    1664773961 - 3876690
    первые 10 символов - дата и время
    следующие 7 символов - доли секунды
    '''
    dateid = int(round(time.time(), 7) * 10000000)
    return dateid


def re_sub_convert_datetime(matchobj):
    out_txt = f"{matchobj.group(2).zfill(4)}-{matchobj.group(3).zfill(2)}-{matchobj.group(4).zfill(2)} {matchobj.group(5).zfill(2)}:{matchobj.group(6).zfill(2)}:{matchobj.group(6).zfill(2)}"
    return out_txt


def is_valid_datetime(string):
    try:
        datetime.datetime.strptime(string, "%Y-%m-%d %H:%M:%S")
        return True
    except:
        return False


def create_unique_file(filename):
    version = 0
    result_file = filename
    # if we have to try more than 1000 times, something is seriously wrong
    while os.path.exists(result_file) and version < 1000:
        result_file = filename + '.' + str(version)
        version += 1
    if version >= 1000:
        raise OSError('cannot create unique file %s.[0-1000]' % filename)
    return result_file


@contextmanager
def temp_open(filename, mode):
    f = open(filename, mode, encoding='utf-8')
    try:
        yield f
    finally:
        f.close()
        os.remove(filename)


def parse_args():
    """parse args for binlog2sql"""

    parser = argparse.ArgumentParser(description='Parse MySQL binlog to SQL you want', add_help=False)
    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('-h', '--host', dest='host', type=str,
                                 help='Host the MySQL database server located', default='127.0.0.1')
    connect_setting.add_argument('-u', '--user', dest='user', type=str,
                                 help='MySQL Username to log in as', default='root')
    connect_setting.add_argument('-p', '--password', dest='password', type=str, nargs='*',
                                 help='MySQL Password to use', default='')
    connect_setting.add_argument('-P', '--port', dest='port', type=int,
                                 help='MySQL port to use', default=3306)
    interval = parser.add_argument_group('interval filter')
    interval.add_argument('--start-file', dest='start_file', type=str, help='Start binlog file to be parsed')
    interval.add_argument('--start-position', '--start-pos', dest='start_pos', type=int,
                          help='Start position of the --start-file', default=4)
    interval.add_argument('--stop-file', '--end-file', dest='end_file', type=str,
                          help="Stop binlog file to be parsed. default: '--start-file'", default='')
    interval.add_argument('--stop-position', '--end-pos', dest='end_pos', type=int,
                          help="Stop position. default: latest position of '--stop-file'", default=0)
    interval.add_argument('--start-datetime', dest='start_time', type=str,
                          help="Start time. format %%Y-%%m-%%d %%H:%%M:%%S", default='')
    interval.add_argument('--stop-datetime', dest='stop_time', type=str,
                          help="Stop Time. format %%Y-%%m-%%d %%H:%%M:%%S;", default='')
    parser.add_argument('--stop-never', dest='stop_never', action='store_true', default=False,
                        help="Continuously parse binlog. default: stop at the latest event when you start.")
    parser.add_argument('--help', dest='help', action='store_true', help='help information', default=False)

    schema = parser.add_argument_group('schema filter')
    schema.add_argument('-d', '--databases', dest='databases', type=str, nargs='*',
                        help='dbs you want to process', default='')
    schema.add_argument('-t', '--tables', dest='tables', type=str, nargs='*',
                        help='tables you want to process', default='')

    event = parser.add_argument_group('type filter')
    event.add_argument('--only-dml', dest='only_dml', action='store_true', default=False,
                       help='only print dml, ignore ddl')
    event.add_argument('--sql-type', dest='sql_type', type=str, nargs='*', default=['INSERT', 'UPDATE', 'DELETE'],
                       help='Sql type you want to process, support INSERT, UPDATE, DELETE.')

    # exclusive = parser.add_mutually_exclusive_group()
    parser.add_argument('-K', '--no-primary-key', dest='no_pk', action='store_true',
                        help='Generate insert sql without primary key if exists', default=False)
    parser.add_argument('-B', '--flashback', dest='flashback', action='store_true',
                        help='Flashback data to start_position of start_file', default=False)
    parser.add_argument('--back-interval', dest='back_interval', type=float, default=1.0,
                        help="Sleep time between chunks of 1000 rollback sql. set it to 0 if do not need sleep")
    parser.add_argument('-CH', '--for_clickhouse', dest='for_clickhouse', action='store_true',
                        help='Make a design for ClickHouse instead of the SQL', default=False)

    return parser


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if not args.start_file:
        args.start_file = ''
    # if not args.start_file:
    #     raise ValueError('Lack of parameter: start_file')
    if args.flashback and args.stop_never:
        raise ValueError('Only one of flashback or stop-never can be True')
    if args.flashback and args.no_pk:
        raise ValueError('Only one of flashback or no_pk can be True')
    if (args.start_time and not is_valid_datetime(args.start_time)) or \
            (args.stop_time and not is_valid_datetime(args.stop_time)):
        raise ValueError('Incorrect datetime argument')
    if not args.password:
        args.password = getpass.getpass()
    else:
        args.password = args.password[0]
    return args


def compare_items(items):
    # caution: if v is NULL, may need to process
    (k, v) = items
    if v is None:
        return '`%s` IS %%s' % k
    else:
        return '`%s`=%%s' % k


def fix_object(value):
    """Fixes python objects so that they can be properly inserted into SQL queries"""
    if isinstance(value, set):
        value = ','.join(value)
    if PY3PLUS and isinstance(value, bytes):
        if type(value) is bytes:
            value = value.hex()
        else:
            # value = value.decode('utf-8', 'backslashreplace')
            # value = value.decode('utf-8', 'ignore')
            value = value.decode('utf-8')
        return value
    elif not PY3PLUS and isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value


def is_dml_event(event):
    if isinstance(event, WriteRowsEvent) or isinstance(event, UpdateRowsEvent) or isinstance(event, DeleteRowsEvent):
        return True
    else:
        return False


def event_type(event):
    t = None
    if isinstance(event, WriteRowsEvent):
        t = 'INSERT'
    elif isinstance(event, UpdateRowsEvent):
        t = 'UPDATE'
    elif isinstance(event, DeleteRowsEvent):
        t = 'DELETE'
    return t


def concat_sql_from_binlog_event(cursor, binlog_event, row=None, e_start_pos=None, flashback=False, no_pk=False, for_clickhouse=False):
    # print('***')
    # print('concat_sql_from_binlog_event - begin')
    if flashback and no_pk:
        raise ValueError('only one of flashback or no_pk can be True')
    if not (isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent)
            or isinstance(binlog_event, DeleteRowsEvent) or isinstance(binlog_event, QueryEvent)):
        raise ValueError('binlog_event must be WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent or QueryEvent')
    sql = ''
    time = ''
    sql_type = ''
    sql_4insert_table = ''
    sql_4insert_values = ''
    log_pos_start = e_start_pos
    log_pos_end = binlog_event.packet.log_pos
    if isinstance(binlog_event, WriteRowsEvent) or isinstance(binlog_event, UpdateRowsEvent) \
            or isinstance(binlog_event, DeleteRowsEvent):
        pattern = generate_sql_pattern(binlog_event, row=row, flashback=flashback, no_pk=no_pk, for_clickhouse=for_clickhouse)
        # print(f"{pattern = }")
        # print(f"{pattern['template'] = }")
        # print(f"{pattern['values'] = }")
        sql_type = pattern['sql_type']
        if sql_type in ('INSERT', 'IN-UPD'):
            sql_4insert_table = pattern['sql_4insert_table']
            # print(f"{sql_4insert_table = }")
            sql_4insert_values = cursor.mogrify(pattern['sql_4insert_values'], pattern['values'])
            # print(f"BEFORE: {sql_4insert_values = }")
            sql_4insert_values = get_correct_sql(sql_4insert_values)
            # print(f"AFTER: {sql_4insert_values = }")
        sql = cursor.mogrify(pattern['template'], pattern['values'])
        # print(f"{sql = }")
        sql = get_correct_sql(sql)
        # print(f"{sql = }")
        #
        time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
        if for_clickhouse is True:
            pass
        else:
            sql += ' #! start %s end %s time %s' % (e_start_pos, binlog_event.packet.log_pos, time)
    elif flashback is False and isinstance(binlog_event, QueryEvent) and binlog_event.query != 'BEGIN' \
            and binlog_event.query != 'COMMIT':
        if for_clickhouse is True:
            if binlog_event.schema:
                sql = 'USE {0};\n'.format(binlog_event.schema)
            sql += '{0};'.format(fix_object(binlog_event.query))
        else:
            if binlog_event.schema:
                sql = 'USE {0};\n'.format(binlog_event.schema)
            sql += '{0};'.format(fix_object(binlog_event.query))
    # print('concat_sql_from_binlog_event - end')
    # print('***')
    return sql, log_pos_start, log_pos_end, get_schema_clickhouse(binlog_event.schema), binlog_event.table, time, sql_type, sql_4insert_table, sql_4insert_values


def generate_sql_pattern(binlog_event, row=None, flashback=False, no_pk=False, for_clickhouse=False):
    template = ''
    values = []
    sql_type = ''
    sql_4insert_table = ''
    sql_4insert_values = ''
    if flashback is True:
        if isinstance(binlog_event, WriteRowsEvent):
            sql_type = 'DELETE'
            if for_clickhouse is True:
                template = 'ALTER TABLE `{0}`.`{1}` DELETE WHERE {2};'.format(
                    get_schema_clickhouse(binlog_event.schema), binlog_event.table,
                    ' AND '.join(map(compare_items, row['values'].items()))
                )
            else:
                template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                    get_schema_clickhouse(binlog_event.schema), binlog_event.table,
                    ' AND '.join(map(compare_items, row['values'].items()))
                )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, DeleteRowsEvent):
            sql_type = 'INSERT'
            if for_clickhouse is True:
                template = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES ({3});'.format(
                    get_schema_clickhouse(binlog_event.schema), binlog_event.table,
                    ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                    ', '.join(['%s'] * len(row['values']))
                )
            else:
                template = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES ({3});'.format(
                    get_schema_clickhouse(binlog_event.schema), binlog_event.table,
                    ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                    ', '.join(['%s'] * len(row['values']))
                )
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, UpdateRowsEvent):
            sql_type = 'UPDATE'
            if for_clickhouse is True:
                template = 'ALTER TABLE `{0}`.`{1}` UPDATE {2} WHERE {3};'.format(
                    get_schema_clickhouse(binlog_event.schema), binlog_event.table,
                    ', '.join(['`%s`=%%s' % x for x in row['before_values'].keys()]),
                    ' AND '.join(map(compare_items, row['after_values'].items())))
            else:
                template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                    get_schema_clickhouse(binlog_event.schema), binlog_event.table,
                    ', '.join(['`%s`=%%s' % x for x in row['before_values'].keys()]),
                    ' AND '.join(map(compare_items, row['after_values'].items())))
            values = map(fix_object, list(row['before_values'].values()) + list(row['after_values'].values()))
    else:
        if isinstance(binlog_event, WriteRowsEvent):
            sql_type = 'INSERT'
            # print(f"{row['values'].keys() = }")
            # print(f"{row['values'].values() = }")
            # print(f"{row['values'] = }")
            if no_pk:
                # print binlog_event.__dict__
                # tableInfo = (binlog_event.table_map)[binlog_event.table_id]
                # if tableInfo.primary_key:
                #     row['values'].pop(tableInfo.primary_key)
                if binlog_event.primary_key:
                    row['values'].pop(binlog_event.primary_key)
            if for_clickhouse is True:
                if binlog_event.table in settings.tables_not_updated:
                    # добавляем поле dateid
                    row['values']['dateid'] = get_dateid()
                template = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES ({3});'.format(
                    get_schema_clickhouse(binlog_event.schema), binlog_event.table,
                    ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                    ', '.join(['%s'] * len(row['values']))
                )
                sql_4insert_table = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES '.format(
                    get_schema_clickhouse(binlog_event.schema), binlog_event.table,
                    ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())))
                sql_4insert_values = '({0})'.format(', '.join(['%s'] * len(row['values'])))
            else:
                template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});'.format(
                    get_schema_clickhouse(binlog_event.schema), binlog_event.table,
                    ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                    ', '.join(['%s'] * len(row['values']))
                )
            values = map(fix_object, row['values'].values())
            # print(f"{row['values'].values() = }")
            # print(f"{values = }")
        elif isinstance(binlog_event, DeleteRowsEvent):
            sql_type = 'DELETE'
            if for_clickhouse is True:
                template = 'ALTER TABLE `{0}`.`{1}` DELETE WHERE {2};'.format(
                    get_schema_clickhouse(binlog_event.schema), binlog_event.table, ' AND '.join(map(compare_items, row['values'].items())))
            else:
                template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(
                    get_schema_clickhouse(binlog_event.schema), binlog_event.table, ' AND '.join(map(compare_items, row['values'].items())))
            values = map(fix_object, row['values'].values())
        elif isinstance(binlog_event, UpdateRowsEvent):
            sql_type = 'UPDATE'
            if for_clickhouse is True:
                # UPDATE в clickhouse работает ОЧЕНЬ МЕДЛЕННО, поэтому в больших таблицах с частыми UPDATE заменяем INSERT
                # ВНИМАНИЕ!!! это важно учитывать когда будем делать селекты
                # чтобы потом корректно с этим работать нужно брать самую свежую запись (максимальное значение поля dateid)
                if binlog_event.table in settings.tables_not_updated:
                    # добавляем поле dateid
                    row['after_values']['dateid'] = get_dateid()
                    sql_type = 'INS-UPD'
                    # print(f"{row['after_values'].keys() = }")
                    # print(f"{row['after_values'].values() = }")
                    template = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES ({3});'.format(
                        get_schema_clickhouse(binlog_event.schema), binlog_event.table,
                        ', '.join(map(lambda key: '`%s`' % key, row['after_values'].keys())),
                        ', '.join(['%s'] * len(row['after_values']))
                    )
                    sql_4insert_table = 'INSERT INTO `{0}`.`{1}` ({2}) VALUES '.format(
                        get_schema_clickhouse(binlog_event.schema), binlog_event.table,
                        ', '.join(map(lambda key: '`%s`' % key, row['after_values'].keys())))
                    sql_4insert_values = '({0})'.format(', '.join(['%s'] * len(row['after_values'])))
                    values = map(fix_object, row['after_values'].values())
                    # print(f"{row['after_values'].values() = }")
                else:
                    # для остальных таблиц оставляем UPDATE
                    # если новое значение=старому, то обновлять его не будем.
                    # ВНИМАНИЕ! это необходимо для того, чтобы первичные ключи не ругались (их нельзя обновлять!)
                    for dv_key_name, dv_key_value in list(row['after_values'].items()):
                        if row['after_values'][dv_key_name] == row['before_values'][dv_key_name]:
                            del row['after_values'][dv_key_name]
                            pass
                    template = 'ALTER TABLE `{0}`.`{1}` UPDATE {2} WHERE {3};'.format(
                        get_schema_clickhouse(binlog_event.schema), binlog_event.table,
                        ', '.join(['`%s`=%%s' % k for k in row['after_values'].keys()]),
                        ' AND '.join(['`%s`=%%s' % k for k in row['before_values'].keys()])
                    )
                    values = map(fix_object, list(row['after_values'].values()) + list(row['before_values'].values()))
            else:
                template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(
                    get_schema_clickhouse(binlog_event.schema), binlog_event.table,
                    ', '.join(['`%s`=%%s' % k for k in row['after_values'].keys()]),
                    ' AND '.join(map(compare_items, row['before_values'].items()))
                )
                values = map(fix_object, list(row['after_values'].values()) + list(row['before_values'].values()))
    return {'sql_type': sql_type, 'template': template, 'sql_4insert_table': sql_4insert_table, 'sql_4insert_values': sql_4insert_values, 'values': list(values)}


def reversed_lines(fin):
    """Generate the lines of file in reverse order."""
    part = ''
    for block in reversed_blocks(fin):
        if PY3PLUS:
            # block = block.decode('utf-8', 'backslashreplace')
            # block = block.decode('utf-8', 'ignore')
            block = block.decode('utf-8')
        for c in reversed(block):
            if c == '\n' and part:
                yield part[::-1]
                part = ''
            part += c
    if part:
        yield part[::-1]


def reversed_blocks(fin, block_size=4096):
    """Generate blocks of file's contents in reverse order."""
    fin.seek(0, os.SEEK_END)
    here = fin.tell()
    while 0 < here:
        delta = min(block_size, here)
        here -= delta
        fin.seek(here, os.SEEK_SET)
        yield fin.read(delta)
