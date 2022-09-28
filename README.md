# matomo2clickhouse

Replication Matomo from MySQL to ClickHouse (Репликация Matomo: переливка данных из MySQL в ClickHouse)

Сначала настроить, потом вручную запускать проект: ```matomo2clickhouse_start.sh```

Для автоматизации можно настроить через cron выполнение команды:

```pipenv run python3 matomo2clickhouse.py```


### Кратко весь процесс настройки:
- Создать таблицы в ClickHouse (всю структуру таблиц)
- Скопировать все уже существующие данные из MySQL в ClickHouse
- Настроить репликацию из MySQL в ClickHouse 


### Установка matomo2clickhouse (выполняем пошагово)

- Для работы потребуется Linux (тестирование проводилось на ubuntu 22.04.01)
- Устанавливаем питон (тестирование данной инструкции проводилось на 3.10, на остальных версиях работу не гарантирую, но должно работать на версиях 3.9+)
- Устанавливаем pip (на linux):

```sudo apt install python3-pip```

- Далее устанавливаем pipenv (на linux):

```pip3 install pipenv```

- Создаем нужный каталог в нужном нам месте
- Копируем в этот каталог файлы проекта https://github.com/dneupokoev/matomo2clickhouse
- Заходим в созданный каталог и создаем в нем пустой каталог .venv
- В каталоге проекта выполняем команды:

```pipenv shell```

```pipenv sync```

- Редактируем файл _settings.py (описание все настроек внутри файла!)
- Настраиваем регулярное выполнение (например, через cron) команды:

```pipenv run python3 matomo2clickhouse.py```


### MySQL
- Matomo может использовать MySQL/MariaDB/Percona или другие DB семейства MySQL, далее будем это всё называть MySQL
- Для работы python с MySQL скорее всего сначала потребуется установить клиентскую библиотеку для ос, поэтому пробуем установить:

```sudo apt install libmysqlclient-dev```

- Для работы репликации в MySQL нужно включить binlog. Внимание: необходимо предусмотреть чтобы было достаточно места на диске для бинлога!

```
Редактируем /etc/mysql/mariadb.conf.d/50-server.cnf (файл может быть в другом месте):

[mysqld]:
default-authentication-plugin = mysql_native_password
server_id = 1
log_bin = /var/log/mysql/mysql-bin.log
max_binlog_size = 100M
expire_logs_days = 30
binlog_format = row
binlog_row_image = full
binlog_do_db = название базы, (можно несколько строк для нескольких баз)
```

- После внесенных изменений рестартуем сервис БД (название сервиса может отличаться):

```sudo systemctl restart mariadb.service```

- В базе MySQL завести пользователя и задать ему права:

```GRANT SELECT, PROCESS, SUPER, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%';```


### ClickHouse
- Для создания структуры выполнить скрипт: script_create_clickhouse_table.sql (ВНИМАНИЕ!!! сначала необходимо изучить скрипт!)
- Если потребуются дополнительные таблицы, то читать описание внутри _settings.py


### Дополнительно
- Если работает экземпляр программы, то второй экземпляр запускаться не будет (отслеживается через создание и проверку наличия файла)  
- Если задать нужные настройки в settings.py, то результат работы будет присылать в телеграм (в личку или указанный канал)
- Записывается лог ошибок
- Можно включить вывод информации о дисковом пространстве

