# -*- coding: utf-8 -*-
# matomo2clickhouse
# https://github.com/dneupokoev/matomo2clickhouse
#
# Replication Matomo from MySQL to ClickHouse
# Репликация Matomo: переливка данных из MySQL в ClickHouse
#
# 230403:
# + добавил параметр settings.EXECUTE_CLICKHOUSE (нужен для тестирования) - True: выполнять insert в ClickHouse (боевой режим); False: не выполнять insert (для тестирования и отладки)
# + изменил параметр CH_matomo_dbname - теперь базы в MySQL и ClickHouse могут иметь разные названия
#
# 221005:
# + базовая стабильная версия (полностью протестированная и отлаженная)
#
#
# ВНИМАНИЕ!!! Перед запуском необходимо ЗАПОЛНИТЬ пароли в данном файле и ПЕРЕИМЕНОВАТЬ его в settings.py
#
# подключение к mysql (matomo)
MySQL_matomo_host = '192.168.5.'
MySQL_matomo_port = 3306
MySQL_matomo_dbname = 'matomo'
MySQL_matomo_user = 'user'
MySQL_matomo_password = 'password'
#
CH_matomo_host = '192.168.5.'
CH_matomo_port = 9000
CH_matomo_dbname = 'matomo'
CH_matomo_user = 'user'
CH_matomo_password = 'password'
#
#
#
# *** Настройки ***
# для избыточного логирования True (для тестирования и отладки), иначе False
# DEBUG = True
DEBUG = False
#
# EXECUTE_CLICKHOUSE - True: выполнять insert в ClickHouse (боевой режим); False: не выполнять insert (для тестирования и отладки)
EXECUTE_CLICKHOUSE = True
# EXECUTE_CLICKHOUSE = False
#
# создаем папку для логов:
# sudo mkdir /var/lib/matomo2clickhouse
# выдаем полные права на папку:
# sudo chmod 777 /var/lib/matomo2clickhouse
PATH_TO_LIB = '/var/lib/matomo2clickhouse/'
#
# создаем папку для переменных данного проекта:
# sudo mkdir /var/log/matomo2clickhouse
# выдаем полные права на папку:
# sudo chmod 777 /var/log/matomo2clickhouse
PATH_TO_LOG = '/var/log/matomo2clickhouse/'
#
#
# Какое максимальное количество запросов обрабатывать за один вызов скрипта
# replication_batch_size - общее количество строк
replication_batch_size = 1000000
#
# replication_batch_sql - строк в одном коннекте (ВНИМАНИЕ! для построчного выполнения = 0)
# Оптимально около 2000. Если сделать слишком мало, то будет медленно. Если сделать слишком много, то либо съест ОЗУ, либо ClickHouse не сможет обработать такой большой запрос.
replication_batch_sql = 2000
#
# Какое максимальное количество файлов binlog-а обрабатывать за один вызов (если поставить слишком много, то может надолго подвиснуть)
replication_max_number_files_per_session = 20
#
# максимальное количество минут работы скрипта до остановки (0 - без остановки, int - может понадобиться, чтобы гибче управлять автозапуском)
# ВНИМАНИЕ! реально может работать на несколько минут дольше указанного
replication_max_minutes = 50
#
#
# LEAVE_BINARY_LOGS_IN_DAYS - оставляем бинарные логи за предыдущие Х дней
# ВНИМАНИЕ! логи чистятся только если последняя точка репликации позже, чем точка в логах для удаления NOW-точка > LEAVE_BINARY_LOGS_IN_DAYS
LEAVE_BINARY_LOGS_IN_DAYS = 180
# sql: PURGE BINARY LOGS BEFORE DATE(NOW() - INTERVAL 30 DAY) + INTERVAL 0 SECOND;
#
#
# Таблицы, которые нужно реплицировать (только эти таблицы будут заливаться в базу-приемник)
# Ключевые таблицы matomo:
# log_visit - содержит одну запись за посещение (данные о сессии: начало, конец, инфа о посетителе, стандартные utm и т.д.)
# log_action - содержит все типы действий, возможных на веб-сайте (например, уникальные URL-адреса, заголовки страниц, URL-адреса загрузки и т.д.)
# log_link_visit_action - содержит одну запись на каждое действие посетителя (просмотр страницы, т.д.)
# log_conversion - содержит конверсии (действия, соответствующие цели), которые произошли во время посещения
# log_conversion_item - содержит элементы конверсии электронной коммерции
#
# Если необходимо добавить таблицы, то нужно СНАЧАЛА СОЗДАТЬ структуру в ClickHouse,
# ( соответствие типов данных MySQL-ClickHouse: https://clickhouse.com/docs/en/engines/database-engines/mysql/ )
# потом залить в CH данные, которые уже есть в MySQL и
# добавить названия таблиц сюда:
replication_tables = [
    'matomo_custom_dimensions',
    'matomo_goal',
    'matomo_log_action',
    'matomo_log_conversion',
    'matomo_log_conversion_item',
    'matomo_log_link_visit_action',
    'matomo_log_profiling',
    'matomo_log_visit',
    'matomo_site',
    'matomo_site_url',
    # 'matomo_tagmanager_container',
    # 'matomo_tagmanager_container_release',
    # 'matomo_tagmanager_container_version',
    # 'matomo_tagmanager_tag',
    # 'matomo_tagmanager_trigger',
    # 'matomo_tagmanager_variable',
]
#
# таблицы, в которые все update будем менять на insert
# ВНИМАНИЕ! в тих таблицах в кликхаусе должно быть поле dateid UInt64
# чтобы потом корректно с этим работать нужно брать самую свежую запись (максимальное значение поля dateid)
tables_not_updated = [
    'matomo_log_visit',
    'matomo_log_link_visit_action',
]
#
#
# True - Проверять свободное место на диске, False - не проверять
CHECK_DISK_SPACE = False
#
#
# TELEGRAM
# True - отправлять результат в телеграм, False - не отправлять
SEND_TELEGRAM = False
# SEND_SUCCESS_REPEATED_NOT_EARLIER_THAN_MINUTES - минимальное количество минут между отправками УСПЕХА (чтобы не заспамить)
SEND_SUCCESS_REPEATED_NOT_EARLIER_THAN_MINUTES = 360
# создать бота - получить токен - создать группу - бота сделать администратором - получить id группы
TLG_BOT_TOKEN = 'your_bot_token'
# TLG_CHAT_FOR_SEND = идентификатор группы
# Как узнать идентификтор группы:
# 1. Добавить бота в нужную группу;
# 2. Написать хотя бы одно сообщение в неё;
# 3. Отправить GET-запрос по следующему адресу:
# curl https://api.telegram.org/bot<your_bot_token>/getUpdates
# 4. Взять значение "id" из объекта "chat". Это и есть идентификатор чата. Для групповых чатов он отрицательный, для личных переписок положительный.
TLG_CHAT_FOR_SEND = -000
#
#
#
#
#
#
#
#
#
#
#
# ВНИМАНИЕ!!! Дальше настройки работы. Перед тем как их трогать НЕОБХОДИМО разобраться в настройках и понимать что к чему!
#
MySQL_connect = [f"-h{MySQL_matomo_host}",
                 f"-P{MySQL_matomo_port}",
                 f"-u{MySQL_matomo_user}",
                 f"-p{MySQL_matomo_password}",
                 f"-d{MySQL_matomo_dbname}",
                 ]
CH_connect = {'host': CH_matomo_host, 'port': CH_matomo_port, 'database': CH_matomo_dbname}
#
#
# Переменная с параметрами для выгрузки бинлога MySQL в ClickHouse
args_for_mysql_to_clickhouse = [''] + \
                               MySQL_connect + \
                               ['-t'] + replication_tables + \
                               ['--for_clickhouse', '--only-dml']
#
#
#
#
#
import telebot


#
#
def f_telegram_send_message(tlg_bot_token='', tlg_chat_id=None, txt_to_send='', txt_mode=None, txt_type='', txt_name=''):
    '''
    функция отправляет в указанный чат телеграма текст
    Входные параметры: токен, чат, текст, тип форматирования текста (HTML, MARKDOWN)
    '''
    if txt_type == 'ERROR':
        txt_type = '❌'
        # txt_type = '\u000274C'
    elif txt_type == 'WARNING':
        txt_type = '⚠'
        # txt_type = '\U0002757'
    elif txt_type == 'INFO':
        txt_type = 'ℹ'
        # txt_type = '\U0002755'
    elif txt_type == 'SUCCESS':
        txt_type = '✅'
        # txt_type = '\U000270'
    else:
        txt_type = ''
    txt_to_send = f"{txt_type} {txt_name} | {txt_to_send}"
    try:
        # dv_tlg_bot = telebot.TeleBot(TLG_BOT_TOKEN, parse_mode=None)
        # dv_tlg_bot = telebot.TeleBot(TLG_BOT_TOKEN, parse_mode='MARKDOWN')
        dv_tlg_bot = telebot.TeleBot(tlg_bot_token, parse_mode=txt_mode)
        # отправляем текст
        tmp_out = dv_tlg_bot.send_message(tlg_chat_id, txt_to_send[0:3999])
        return f"chat_id = {tlg_chat_id} | message_id = {tmp_out.id} | html_text = '{tmp_out.html_text}'"
    except Exception as error:
        return f"ERROR: {error}"
