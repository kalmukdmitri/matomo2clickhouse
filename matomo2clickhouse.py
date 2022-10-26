# -*- coding: utf-8 -*-
# matomo2clickhouse
# https://github.com/dneupokoev/matomo2clickhouse
dv_file_version = '221026.01'
#
# Replication Matomo from MySQL to ClickHouse
# Репликация Matomo: переливка данных из MySQL в ClickHouse

import settings
import os
import re
import sys
import datetime
import time
import pymysql
import configparser
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent
from binlog2sql_util import command_line_args, concat_sql_from_binlog_event, create_unique_file, temp_open, reversed_lines, is_dml_event, event_type, get_dateid
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

# импортируем библиотеку для логирования
from loguru import logger

# logger.add("log/" + dv_file_name + ".json", level="DEBUG", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8", serialize=True)
# logger.add("log/" + dv_file_name + ".json", level="WARNING", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8", serialize=True)
# logger.add("log/" + dv_file_name + ".json", level="INFO", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8", serialize=True)
logger.remove()  # отключаем логирование в консоль
if settings.DEBUG is True:
    logger.add(settings.PATH_TO_LOG + dv_file_name + ".log", level="DEBUG", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8")
    logger.add(sys.stderr, level="DEBUG")
else:
    logger.add(settings.PATH_TO_LOG + dv_file_name + ".log", level="INFO", rotation="00:00", retention='30 days', compression="gz", encoding="utf-8")
    logger.add(sys.stderr, level="INFO")
logger.enable(dv_file_name)  # даем имя логированию
logger.info(f'***')
logger.info(f'BEGIN')
logger.info(f'{dv_path_main = }')
logger.info(f'{dv_file_name = }')
logger.info(f'{dv_file_version = }')
#
dv_find_text = re.compile(r'(\r|\n|\t|\b)')


#
#
#
def get_now():
    '''
    вернет текущую дату и время в заданном формате
    '''
    dv_time_begin = time.time()
    dv_created = f"{datetime.datetime.fromtimestamp(dv_time_begin).strftime('%Y-%m-%d %H:%M:%S')}"
    # dv_created = f"{datetime.datetime.fromtimestamp(dv_time_begin).strftime('%Y-%m-%d %H:%M:%S.%f')}"
    return dv_created


def get_second_between_now_and_datetime(in_datetime_str='2000-01-01 00:00:00'):
    '''
    вернет количество секунд между текущим временем и полученной датой-временем в формате '%Y-%m-%d %H:%M:%S'
    '''
    tmp_datetime_start = datetime.datetime.strptime(in_datetime_str, '%Y-%m-%d %H:%M:%S')
    tmp_now = datetime.datetime.strptime(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    tmp_seconds = int((tmp_now - tmp_datetime_start).total_seconds())
    return tmp_seconds


def get_disk_space():
    '''
    вернет информацию о свободном месте на диске в гигабайтах
    dv_statvfs_bavail = Количество свободных гагабайтов, которые разрешено использовать обычным пользователям (исключая зарезервированное пространство)
    dv_statvfs_blocks = Размер файловой системы в гигабайтах
    dv_result_bool = true - корректно отработало, false - получить данные не удалось
    '''
    dv_statvfs_blocks = 999999
    dv_statvfs_bavail = dv_statvfs_blocks
    dv_result_bool = False
    try:
        # получаем свободное место на диске ubuntu
        statvfs = os.statvfs('/')
        # Size of filesystem in bytes (Размер файловой системы в байтах)
        dv_statvfs_blocks = round((statvfs.f_frsize * statvfs.f_blocks) / (1024 * 1024 * 1024), 2)
        # Actual number of free bytes (Фактическое количество свободных байтов)
        # dv_statvfs_bfree = round((statvfs.f_frsize * statvfs.f_bfree) / (1024 * 1024 * 1024), 2)
        # Number of free bytes that ordinary users are allowed to use (excl. reserved space)
        # Количество свободных байтов, которые разрешено использовать обычным пользователям (исключая зарезервированное пространство)
        dv_statvfs_bavail = round((statvfs.f_frsize * statvfs.f_bavail) / (1024 * 1024 * 1024), 2)
        dv_result_bool = True
    except:
        pass
    return dv_statvfs_bavail, dv_statvfs_blocks, dv_result_bool


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
            raise ValueError('no table "log_replication" in database ClickHouse or problems...')
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

            # print(f'{self.server_id = }')
            # print(f'{self.start_file = }')
            # print(f'{self.start_pos = }')
            # print(f'{self.end_file = }')
            # print(f'{self.end_pos = }')
            # print(f'{self.eof_file = }')
            # print(f'{self.eof_pos = }')
            # print(f'{self.binlogList = }')

    def clear_binlog(self, log_time):
        # print(f"{log_time = }")
        tmp_LEAVE_BINARY_LOGS_IN_DAYS = datetime.datetime.today() - datetime.timedelta(days=settings.LEAVE_BINARY_LOGS_IN_DAYS)
        # print(f"{tmp_LEAVE_BINARY_LOGS_IN_DAYS = }")
        if log_time > tmp_LEAVE_BINARY_LOGS_IN_DAYS:
            self.connection = pymysql.connect(**self.conn_mysql_setting)
            with self.connection.cursor() as cursor:
                tmp_sql_execute = f"PURGE BINARY LOGS BEFORE DATE(NOW() - INTERVAL {settings.LEAVE_BINARY_LOGS_IN_DAYS} DAY) + INTERVAL 0 SECOND"
                # print(f"{tmp_sql_execute = }")
                cursor.execute(tmp_sql_execute)
                logger.info(f"{tmp_sql_execute}")

    def process_binlog(self):
        dv_time_begin = time.time()
        dv_count_sql_for_ch = 0
        try:
            stream = BinLogStreamReader(connection_settings=self.conn_mysql_setting, server_id=self.server_id,
                                        log_file=self.start_file, log_pos=self.start_pos, only_schemas=self.only_schemas,
                                        only_tables=self.only_tables, resume_stream=True, blocking=True,
                                        is_mariadb=False, freeze_schema=True)
            flag_last_event = False
            dv_sql_for_execute_list = ''
            dv_sql_for_execute_last = ''
            if self.flashback:
                self.log_id = self.log_id - settings.replication_batch_size
                if self.log_id < 0:
                    dv_count_sql_for_ch = dv_count_sql_for_ch - self.log_id
                    self.log_id = 0
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
                            dv_count_sql_for_ch += 1
                            sql, log_pos_start, log_pos_end, log_shema, log_table, log_time, sql_type = concat_sql_from_binlog_event(cursor=cursor,
                                                                                                                                     binlog_event=binlog_event,
                                                                                                                                     no_pk=self.no_pk,
                                                                                                                                     row=row,
                                                                                                                                     flashback=self.flashback,
                                                                                                                                     e_start_pos=e_start_pos,
                                                                                                                                     for_clickhouse=self.for_clickhouse)
                            if self.for_clickhouse is True:
                                pass
                            else:
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
                                self.log_id += 1
                                dv_sql_log = "ALTER TABLE `%s`.`log_replication` DELETE WHERE `dateid`=%s;" % (log_shema, self.log_id)
                                # f_tmp.write(dv_sql_log + '\n' + sql + '\n')
                                # f_tmp.write(sql + '\n')
                                f_tmp.write(dv_sql_log + '\n')
                            else:
                                self.log_id += 1
                                dv_sql_log = "INSERT INTO `%s`.`log_replication` (`dateid`,`log_time`,`log_file`,`log_pos_start`,`log_pos_end`,`sql_type`)" \
                                             " VALUES (%s,'%s','%s',%s,%s,'%s');" \
                                             % (log_shema, get_dateid(), log_time, stream.log_file, int(log_pos_start), int(log_pos_end), sql_type)
                                #
                                logger.debug(f"execute sql to clickhouse | begin")
                                if (settings.replication_batch_sql == 0) or (len(sql.splitlines()) > 1) or (dv_find_text.search(sql)):
                                    # сюда попадаем если в настройках указали обработку ПОСТРОЧНО
                                    # или в запросе есть перевод каретки (СТРОК в запросе > 1)
                                    # или в запросе есть управляющий символ
                                    #
                                    if dv_sql_for_execute_list != '':
                                        # если ранее уже собрали список запросов, то сначала надо обработать этот список (иначе могут образоваться пропуски данных)
                                        dv_sql_list_for_execute = dv_sql_for_execute_list.splitlines()
                                        with Client(**self.conn_clickhouse_setting) as ch_cursor:
                                            for dv_sql_line in range(len(dv_sql_list_for_execute)):
                                                logger.debug(f"{dv_sql_list_for_execute[dv_sql_line] = }")
                                                # зададим значение dv_sql_for_execute_last, чтобы в случае ошибки знать на каком именно запросе сломалось
                                                dv_sql_for_execute_last = dv_sql_list_for_execute[dv_sql_line]
                                                # выполняем строку sql
                                                ch_cursor.execute(dv_sql_list_for_execute[dv_sql_line])
                                        dv_sql_for_execute_list = ''
                                    # далее обрабатываем текущую строку
                                    with Client(**self.conn_clickhouse_setting) as ch_cursor:
                                        dv_sql_for_execute_last = sql
                                        logger.debug(f"{dv_sql_for_execute_last = }")
                                        # выполняем строку sql
                                        ch_cursor.execute(dv_sql_for_execute_last)
                                        dv_sql_for_execute_last = dv_sql_log
                                        logger.debug(f"{dv_sql_for_execute_last = }")
                                        # выполняем строку sql
                                        ch_cursor.execute(dv_sql_for_execute_last)
                                else:
                                    # пополняем список запросов и если требуется, то будем его обрабатывать
                                    dv_sql_for_execute_list = dv_sql_for_execute_list + sql + '\n' + dv_sql_log + '\n'
                                    if (dv_sql_for_execute_list.count('\n') >= settings.replication_batch_sql) or \
                                            (dv_count_sql_for_ch >= settings.replication_batch_size):
                                        # попадаем сбда если в запрос собрали строк больше, чем replication_batch_sql
                                        # или обработали уже больше replication_batch_size запросов
                                        # (это нужно чтобы не слишком много съедать памяти)
                                        dv_sql_list_for_execute = dv_sql_for_execute_list.splitlines()
                                        with Client(**self.conn_clickhouse_setting) as ch_cursor:
                                            for dv_sql_line in range(len(dv_sql_list_for_execute)):
                                                logger.debug(f"{dv_sql_list_for_execute[dv_sql_line] = }")
                                                # зададим значение dv_sql_for_execute_last, чтобы в случае ошибки знать на каком именно запросе сломалось
                                                dv_sql_for_execute_last = dv_sql_list_for_execute[dv_sql_line]
                                                # выполняем строку sql
                                                ch_cursor.execute(dv_sql_list_for_execute[dv_sql_line])
                                        dv_sql_for_execute_list = ''
                                logger.debug(f"execute sql to clickhouse | end")
                    #
                    # если обработали заданное "максимальное количество запросов обрабатывать за один вызов", то прерываем цикл
                    if dv_count_sql_for_ch >= settings.replication_batch_size:
                        break
                    #
                    # если обрабатывали дольше отведенного времени, то прерываем цикл
                    dv_f_work_munutes = round(int('{:.0f}'.format(1000 * (time.time() - dv_time_begin))) / (1000 * 60))
                    if dv_f_work_munutes >= settings.replication_max_minutes:
                        break
                    #
                    if not (isinstance(binlog_event, RotateEvent) or isinstance(binlog_event, FormatDescriptionEvent)):
                        last_pos = binlog_event.packet.log_pos
                    if flag_last_event:
                        break
                #
                stream.close()
                f_tmp.close()
                if self.flashback:
                    self.print_rollback_sql(filename=tmp_file)
                #
                # чистим старые логи
                if settings.LEAVE_BINARY_LOGS_IN_DAYS > 0 and settings.LEAVE_BINARY_LOGS_IN_DAYS < 99:
                    self.clear_binlog(log_time=log_time)
        #
        except Exception as ERROR:
            f_status = 'ERROR'
            if dv_sql_for_execute_last != '':
                f_text = f"'ERROR = LAST_SQL:\n\n{dv_sql_for_execute_last}\n\n{ERROR = }"
            else:
                f_text = f"{ERROR}"
        else:
            work_time_ms = f"{'{:.0f}'.format(1000 * (time.time() - dv_time_begin))}"
            f_status = 'SUCCESS'
            f_text = f"{f_status} = Успешно обработано {dv_count_sql_for_ch} строк за {work_time_ms} мс. | max_log_time = {log_time}"

        return f_status, f_text

    def print_rollback_sql(self, filename):
        """print rollback sql from tmp_file"""
        with open(filename, mode="rb", encoding='utf-8') as f_tmp:
            batch_size = 1000
            i = 0
            for line in reversed_lines(f_tmp):
                print(line.rstrip())
                with Client(**self.conn_clickhouse_setting) as ch_cursor:
                    ch_cursor.execute(line.rstrip())
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
    # print(f"WW - {is_dml_event(binlog_event) = }")
    try:
        dv_ch_client = Client(**connection_clickhouse_setting)
        dv_ch_execute = dv_ch_client.execute(f"SELECT max(dateid) AS id_max FROM {settings.CH_matomo_dbname}.log_replication")
    except Exception as error:
        raise error
    #
    try:
        log_id_max = dv_ch_execute[0][0]
        ch_result = dv_ch_client.execute(
            f"SELECT dateid, log_time, log_file, log_pos_end FROM {settings.CH_matomo_dbname}.log_replication WHERE dateid={log_id_max}")
        log_time = f"{ch_result[0][1].strftime('%Y-%m-%d %H:%M:%S')}"
        log_file = f"{ch_result[0][2]}"
        log_pos_end = int(ch_result[0][3])
    except:
        pass

    return log_id_max, log_time, log_file, log_pos_end


if __name__ == '__main__':
    dv_time_begin = time.time()
    # получаем информацию о свободном месте на диске в гигабайтах
    dv_disk_space_free_begin = get_disk_space()[0]
    logger.info(f"{dv_disk_space_free_begin = } Gb")
    #
    logger.info(f"{datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')}")
    dv_for_send_txt_type = ''
    dv_for_send_text = ''
    try:
        # читаем значения из конфига
        dv_lib_path_ini = f"{settings.PATH_TO_LIB}/matomo2clickhouse.cfg"
        dv_cfg = configparser.ConfigParser()
        if os.path.exists(dv_lib_path_ini):
            with open(dv_lib_path_ini, mode="r", encoding='utf-8') as fp:
                dv_cfg.read_file(fp)
        # читаем значения
        dv_cfg_last_send_tlg_success = dv_cfg.get('DEFAULT', 'last_send_tlg_success', fallback='2000-01-01 00:00:00')
    except:
        pass
    #
    try:
        dv_file_lib_path = f"{settings.PATH_TO_LIB}/matomo2clickhouse.dat"
        if os.path.exists(dv_file_lib_path):
            dv_file_lib_open = open(dv_file_lib_path, mode="r", encoding='utf-8')
            dv_file_lib_time = next(dv_file_lib_open).strip()
            dv_file_lib_open.close()
            dv_file_old_start = datetime.datetime.strptime(dv_file_lib_time, '%Y-%m-%d %H:%M:%S')
            tmp_now = datetime.datetime.strptime(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
            tmp_seconds = int((tmp_now - dv_file_old_start).total_seconds())
            if tmp_seconds < settings.replication_max_minutes * 2 * 60:
                raise Exception(f"Уже выполняется c {dv_file_lib_time} - перед запуском дождитесь завершения предыдущего процесса!")
        else:
            dv_file_lib_open = open(dv_file_lib_path, mode="w", encoding='utf-8')
            dv_file_lib_time = f"{datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')}"
            dv_file_lib_open.write(f"{dv_file_lib_time}")
            dv_file_lib_open.close()
        #
        # если скрипт вызвали с параметрами, то будем обрабатывать параметры, если без параметров, то возьмем предустановленные параметры из settings.py
        if sys.argv[1:] != []:
            # print(f"{sys.argv[1:] = }")
            in_args = sys.argv[1:]
        else:
            # print(f"{settings.args_for_mysql_to_clickhouse = }")
            in_args = settings.args_for_mysql_to_clickhouse[1:]
        # parse args
        args = command_line_args(in_args)
        # conn_mysql_setting = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8'}
        conn_mysql_setting = {'host': args.host, 'port': args.port, 'user': args.user, 'passwd': args.password, 'charset': 'utf8mb4'}
        conn_clickhouse_setting = settings.CH_connect
        #
        log_id = 0
        if args.start_file == '':
            log_id, log_time, log_file, log_pos_end = get_ch_param_for_next(connection_clickhouse_setting=conn_clickhouse_setting)
            # print(f"{log_id = }")
            # print(f"{log_time = }")
            # print(f"{log_file = }")
            # print(f"{log_pos_end = }")
            if log_file != '':
                args.start_file = log_file
                args.start_pos = log_pos_end
                args.start_time = log_time
        #
        # print('***')
        # print(f"{args = }")
        # print('***')
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
        dv_for_send_txt_type, dv_for_send_text = binlog2sql.process_binlog()
        os.remove(dv_file_lib_path)
    except Exception as ERROR:
        dv_for_send_txt_type = 'ERROR'
        dv_for_send_text = f"{ERROR = }"
    finally:
        # получаем информацию о свободном месте на диске в гигабайтах
        dv_disk_space_free_end, dv_statvfs_blocks, dv_result_bool = get_disk_space()
        logger.info(f"{dv_disk_space_free_end = } Gb")
        try:
            if settings.CHECK_DISK_SPACE is True:
                # формируем текст о состоянии места на диске
                if dv_result_bool is True:
                    dv_for_send_text = f"{dv_for_send_text} | disk space all/free_begin/free_end: {dv_statvfs_blocks}/{dv_disk_space_free_begin}/{dv_disk_space_free_end} Gb"
                else:
                    dv_for_send_text = f"{dv_for_send_text} | check_disk_space = ERROR"
        except:
            pass
        logger.info(f"{dv_for_send_text}")
        try:
            if settings.SEND_TELEGRAM is True:
                if dv_for_send_txt_type == 'ERROR':
                    # если отработало с ошибкой, то в телеграм оправляем ошибку ВСЕГДА! хоть каждую минуту - это ен спам
                    dv_is_SEND_TELEGRAM_success = True
                else:
                    dv_is_SEND_TELEGRAM_success = False
                    # Чтобы слишком часто не спамить в телеграм сначала проверим разрешено ли именно сейчас отправлять сообщение
                    try:
                        # проверяем нужно ли отправлять успех в телеграм
                        if get_second_between_now_and_datetime(dv_cfg_last_send_tlg_success) > settings.SEND_SUCCESS_REPEATED_NOT_EARLIER_THAN_MINUTES * 60:
                            dv_is_SEND_TELEGRAM_success = True
                            # актуализируем значение конфига
                            dv_cfg_last_send_tlg_success = get_now()
                            dv_cfg.set('DEFAULT', 'last_send_tlg_success', dv_cfg_last_send_tlg_success)
                        else:
                            dv_is_SEND_TELEGRAM_success = False
                    except:
                        dv_is_SEND_TELEGRAM_success = False
                #
                # пытаться отправить будем только если предыдущие проверки подтвердили необходимость отправки
                if dv_is_SEND_TELEGRAM_success is True:
                    settings.f_telegram_send_message(tlg_bot_token=settings.TLG_BOT_TOKEN, tlg_chat_id=settings.TLG_CHAT_FOR_SEND,
                                                     txt_name=f"matomo2clickhouse {dv_file_version}",
                                                     txt_type=dv_for_send_txt_type,
                                                     txt_to_send=f"{dv_for_send_text}",
                                                     txt_mode=None)
        except:
            pass
        try:
            # сохраняем файл конфига
            with open(dv_lib_path_ini, mode='w', encoding='utf-8') as configfile:
                dv_cfg.write(configfile)
        except:
            pass
        #
        logger.info(f"{datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S.%f')}")
        work_time_ms = int('{:.0f}'.format(1000 * (time.time() - dv_time_begin)))
        logger.info(f"{work_time_ms = }")
        #
        logger.info(f'END')
