# -*- coding: utf-8 -*-
# Replication Matomo from MySQL to ClickHouse
# Репликация Matomo: переливка данных из MySQL в ClickHouse
# 220916

import sys
import settings
import datetime
import time
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent
from binlog2sql_util import command_line_args, concat_sql_from_binlog_event, create_unique_file, temp_open, reversed_lines, is_dml_event, event_type
from clickhouse_driver import Client
#
#
from pathlib import Path

try:  # from project
    dv_path_main = f"{Path(__file__).parent}/"
    dv_file_name = f"{Path(__file__).name}"
except:  # from jupiter
    dv_path_main = f"{Path.cwd()}/"
    dv_path_main = dv_path_main.replace('jupyter/', '')
    dv_file_name = 'unknown_file'


#
#
def get_now():
    '''
    вернет текущую дату и вермя в заданном формате
    '''
    dv_time_begin = time.time()
    dv_created = f"{datetime.datetime.fromtimestamp(dv_time_begin).strftime('%Y-%m-%d %H:%M:%S')}"
    # dv_created = f"{datetime.datetime.fromtimestamp(dv_time_begin).strftime('%Y-%m-%d %H:%M:%S.%f')}"
    return dv_created


class Binlog2sql(object):

    def __init__(self, connection_mysql_setting, connection_clickhouse_setting,
                 start_file=None, start_pos=None, end_file=None, end_pos=None,
                 start_time=None, stop_time=None, only_schemas=None, only_tables=None, no_pk=False,
                 flashback=False, stop_never=False, back_interval=1.0, only_dml=True, sql_type=None, for_clickhouse=False,
                 log_id=None):
        """
        conn_mysql_setting: {'host': 127.0.0.1, 'port': 3306, 'user': user, 'passwd': passwd, 'charset': 'utf8'}
        """

        if log_id is None:
            raise ValueError('no table "last_replication" in database ClickHouse or problems...')
        else:
            self.log_id = int(log_id)

        self.conn_clickhouse_setting = connection_clickhouse_setting
        # dv_ch_client = Client(**self.conn_clickhouse_setting)
        # result = dv_ch_client.execute("SHOW DATABASES")
        # print(f"{result = }")


        self.conn_mysql_setting = connection_mysql_setting

        if not start_file:
            self.connection = pymysql.connect(**self.conn_mysql_setting)
            with self.connection.cursor() as cursor:
                cursor.execute("SHOW MASTER LOGS")
                binlog_start_file = cursor.fetchone()[:1]
                self.start_file = binlog_start_file[0]
            # raise ValueError('Lack of parameter: start_file')
        else:
            self.start_file = start_file


        self.start_pos = start_pos if start_pos else 4  # use binlog v4
        self.end_file = end_file
        self.end_pos = end_pos
        if start_time:
            self.start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        else:
            self.start_time = datetime.datetime.strptime('1980-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
        if stop_time:
            self.stop_time = datetime.datetime.strptime(stop_time, "%Y-%m-%d %H:%M:%S")
        else:
            self.stop_time = datetime.datetime.strptime('2999-12-31 00:00:00', "%Y-%m-%d %H:%M:%S")

        self.only_schemas = only_schemas if only_schemas else None
        self.only_tables = only_tables if only_tables else None
        self.no_pk, self.flashback, self.stop_never, self.back_interval = (no_pk, flashback, stop_never, back_interval)
        self.for_clickhouse = for_clickhouse
        # print(f"{self.for_clickhouse = }")
        self.only_dml = only_dml
        self.sql_type = [t.upper() for t in sql_type] if sql_type else []

        self.binlogList = []
        self.connection = pymysql.connect(**self.conn_mysql_setting)
        with self.connection.cursor() as cursor:
            cursor.execute("SHOW MASTER STATUS")
            self.eof_file, self.eof_pos = cursor.fetchone()[:2]
            if self.end_pos == 0:
                self.end_pos = self.eof_pos
            if self.end_file == '':
                self.end_file = self.eof_file

            cursor.execute("SHOW MASTER LOGS")
            bin_index = [row[0] for row in cursor.fetchall()]
            if self.start_file not in bin_index:
                raise ValueError('parameter error: start_file %s not in mysql server' % self.start_file)
            binlog2i = lambda x: x.split('.')[1]
            for binary in bin_index:
                if binlog2i(self.start_file) <= binlog2i(binary) <= binlog2i(self.end_file):
                    self.binlogList.append(binary)
            # оставляем список только из количества файлов, которые разрешили для обработки за 1 раз (в settings.py)
            self.binlogList = self.binlogList[:settings.replication_max_number_files_per_session]

            cursor.execute("SELECT @@server_id")
            self.server_id = cursor.fetchone()[0]
            if not self.server_id:
                raise ValueError('missing server_id in %s:%s' % (self.conn_mysql_setting['host'], self.conn_mysql_setting['port']))

            print(f'{self.server_id = }')
            print(f'{self.start_file = }')
            print(f'{self.start_pos = }')
            print(f'{self.end_file = }')
            print(f'{self.end_pos = }')
            print(f'{self.eof_file = }')
            print(f'{self.eof_pos = }')
            print(f'{self.binlogList = }')


    def process_binlog(self):
        stream = BinLogStreamReader(connection_settings=self.conn_mysql_setting, server_id=self.server_id,
                                    log_file=self.start_file, log_pos=self.start_pos, only_schemas=self.only_schemas,
                                    only_tables=self.only_tables, resume_stream=True, blocking=True)

        flag_last_event = False
        tmp_count_for = 0
        e_start_pos, last_pos = stream.log_pos, stream.log_pos
        # to simplify code, we do not use flock for tmp_file.
        tmp_file = create_unique_file('%s.%s' % (self.conn_mysql_setting['host'], self.conn_mysql_setting['port']))
        with temp_open(tmp_file, "w") as f_tmp, self.connection.cursor() as cursor:
            for binlog_event in stream:
                if not self.stop_never:
                    try:
                        event_time = datetime.datetime.fromtimestamp(binlog_event.timestamp)
                    except OSError:
                        event_time = datetime.datetime(1980, 1, 1, 0, 0)
                    if (stream.log_file == self.end_file and stream.log_pos == self.end_pos) or \
                            (stream.log_file == self.eof_file and stream.log_pos == self.eof_pos):
                        flag_last_event = True
                    elif event_time < self.start_time:
                        if not (isinstance(binlog_event, RotateEvent)
                                or isinstance(binlog_event, FormatDescriptionEvent)):
                            last_pos = binlog_event.packet.log_pos
                        continue
                    elif (stream.log_file not in self.binlogList) or \
                            (self.end_pos and stream.log_file == self.end_file and stream.log_pos > self.end_pos) or \
                            (stream.log_file == self.eof_file and stream.log_pos > self.eof_pos) or \
                            (event_time >= self.stop_time):
                        break
                    # else:
                    #     raise ValueError('unknown binlog file or position')

                if isinstance(binlog_event, QueryEvent) and binlog_event.query == 'BEGIN':
                    e_start_pos = last_pos

                if isinstance(binlog_event, QueryEvent) and not self.only_dml:
                    sql, log_pos_start, log_pos_end, log_shema, log_table, log_time = concat_sql_from_binlog_event(cursor=cursor,
                                                                                                                   binlog_event=binlog_event,
                                                                                                                   no_pk=self.no_pk,
                                                                                                                   flashback=self.flashback,
                                                                                                                   for_clickhouse=self.for_clickhouse)
                    if sql:
                        print(sql)
                elif is_dml_event(binlog_event) and event_type(binlog_event) in self.sql_type:
                    for row in binlog_event.rows:
                        tmp_count_for += 1
                        sql, log_pos_start, log_pos_end, log_shema, log_table, log_time = concat_sql_from_binlog_event(cursor=cursor,
                                                                                                                       binlog_event=binlog_event,
                                                                                                                       no_pk=self.no_pk,
                                                                                                                       row=row, flashback=self.flashback,
                                                                                                                       e_start_pos=e_start_pos,
                                                                                                                       for_clickhouse=self.for_clickhouse)
                        sql += ' file %s' % (stream.log_file)

                        # print(dv_sql_log)
                        # print(sql)
                        # print(f"{log_shema = }")
                        # print(f"{log_table = }")
                        # print(f"{stream.log_file = }")
                        # print(f"{log_pos_start = }")
                        # print(f"{log_pos_end = }")
                        # print(f"{log_time = }")
                        if self.flashback:
                            # dv_sql_log = "ALTER INTO `%s`.`last_replication` DELETE WHERE `id`=%s" % (log_shema, self.log_id)
                            # self.log_id -= 1
                            # f_tmp.write(dv_sql_log + '\n' + sql + '\n')
                            f_tmp.write(sql + '\n')
                        else:
                            self.log_id += 1
                            dv_sql_log = "INSERT INTO `%s`.`last_replication` (`id`, `log_time`, `log_file`, `log_pos_start`, `log_pos_end`)" \
                                         " VALUES (%s, '%s', '%s', %s, %s)" \
                                         % (log_shema, self.log_id, log_time, stream.log_file, int(log_pos_start), int(log_pos_end))
                            print(sql)
                            print(dv_sql_log)
                            dv_ch_client = Client(**self.conn_clickhouse_setting)
                            # dv_ch_client.execute(sql)
                            dv_ch_client.execute(dv_sql_log)
                #
                # если обработали заданное "максимальное количество запросов обрабатывать за один вызов", то прерываем цикл
                if tmp_count_for >= settings.replication_batch_size:
                    break

                if not (isinstance(binlog_event, RotateEvent) or isinstance(binlog_event, FormatDescriptionEvent)):
                    last_pos = binlog_event.packet.log_pos
                if flag_last_event:
                    break

            stream.close()
            f_tmp.close()
            if self.flashback:
                self.print_rollback_sql(filename=tmp_file)
        return True

    def print_rollback_sql(self, filename):
        """print rollback sql from tmp_file"""
        with open(filename, "rb") as f_tmp:
            batch_size = 1000
            i = 0
            for line in reversed_lines(f_tmp):
                print(line.rstrip())
                if i >= batch_size:
                    i = 0
                    if self.back_interval:
                        print('SELECT SLEEP(%s);' % self.back_interval)
                else:
                    i += 1

    def __del__(self):
        pass



def get_ch_param_for_next(connection_clickhouse_setting):
    log_id_max = -1
    log_time = '1980-01-01 00:00:00'
    log_file = ''
    log_pos_end = 0
    #print(f"WW - {is_dml_event(binlog_event) = }")
    try:
        dv_ch_client = Client(**connection_clickhouse_setting)
        dv_ch_execute = dv_ch_client.execute(f"SELECT max(id) AS id_max FROM {settings.CH_matomo_dbname}.last_replication")
    except Exception as error:
        raise error
    #
    try:
        log_id_max = dv_ch_execute[0][0]
        ch_result = dv_ch_client.execute(f"SELECT id, log_time, log_file, log_pos_end FROM {settings.CH_matomo_dbname}.last_replication WHERE id={log_id_max}")
        log_time = f"{ch_result[0][1].strftime('%Y-%m-%d %H:%M:%S')}"
        log_file = f"{ch_result[0][2]}"
        log_pos_end = int(ch_result[0][3])
    except:
        pass

    return log_id_max, log_time, log_file, log_pos_end


if __name__ == '__main__':
    # если скрипт вызвали с параметрами, то будем обрабатывать параметры, если без параметров, то возьмем предустановленные параметры из settings.py
    if sys.argv[1:] != []:
        # print(f"{sys.argv[1:] = }")
        in_args = sys.argv[1:]
    else:
        # print(f"{settings.args_for_mysql_to_clickhouse = }")
        in_args = settings.args_for_mysql_to_clickhouse[1:]
    # parse args
    args = command_line_args(in_args)
    # print('***')
    # print(f"{args = }")
    # print('***')
    conn_mysql_setting = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8'}
    conn_clickhouse_setting = settings.CH_connect
    #
    log_id = 0
    if args.start_file == '':
        log_id, log_time, log_file, log_pos_end = get_ch_param_for_next(connection_clickhouse_setting=conn_clickhouse_setting)
        print(f"{log_id = }")
        print(f"{log_time = }")
        print(f"{log_file = }")
        print(f"{log_pos_end = }")
        if log_file != '':
            args.start_file = log_file
            args.start_pos = log_pos_end
            args.start_time = log_time
    #
    print('***')
    print(f"{args = }")
    print('***')
    #
    binlog2sql = Binlog2sql(connection_mysql_setting=conn_mysql_setting,
                            connection_clickhouse_setting=conn_clickhouse_setting,
                            start_file=args.start_file, start_pos=args.start_pos,
                            end_file=args.end_file, end_pos=args.end_pos,
                            start_time=args.start_time, stop_time=args.stop_time,
                            only_schemas=args.databases, only_tables=args.tables,
                            no_pk=args.no_pk, flashback=args.flashback, stop_never=args.stop_never,
                            back_interval=args.back_interval, only_dml=args.only_dml,
                            sql_type=args.sql_type, for_clickhouse=args.for_clickhouse,
                            log_id=log_id)
    binlog2sql.process_binlog()


