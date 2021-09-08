import os
import psycopg2
from psycopg2 import Error
from psycopg2 import sql


def createTableOfSongs(tableName, dbName):
    try:
        # Подключиться к существующей базе данных
        connection = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password="password",
                                      host="127.0.0.1",
                                      port="5432",
                                      database=dbName)

        cursor = connection.cursor()

        # Выполнение команды: это создает новую таблицу
        cursor.execute(sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} (ID INT PRIMARY KEY NOT NULL, DIRECTORY VARCHAR NOT NULL, SONG VARCHAR NOT NULL, ARTIST VARCHAR NOT NULL)").format(
            sql.Identifier(tableName)))
        connection.commit()
        print("Таблица успешно создана в PostgreSQL")
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


def createTableOfGenres(dbName):
    try:
        # Подключиться к существующей базе данных
        connection = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password="password",
                                      host="127.0.0.1",
                                      port="5432",
                                      database=dbName)

        cursor = connection.cursor()

        # Выполнение команды: это создает новую таблицу
        cursor.execute("CREATE TABLE IF NOT EXISTS genres (ID INT PRIMARY KEY NOT NULL, GENRE VARCHAR NOT NULL)")
        connection.commit()
        print("Таблица успешно создана в PostgreSQL")
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


def fill_with_songs(directory_name, tableName, artistName, dbName):
    try:
        # Подключиться к существующей базе данных
        connection = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password="password",
                                      host="127.0.0.1",
                                      port="5432",
                                      database=dbName)

        cursor = connection.cursor()

        for dirpath, dirnames, filenames in os.walk(directory_name):
            # перебрать файлы
            for filename in filenames:
                song = os.path.join(dirpath, filename)
                if song[-3:] == "mp3":
                    # Выполнение SQL-запроса для вставки данных в таблицу
                    cursor.execute(sql.SQL(
                        "INSERT INTO {} (ID, DIRECTORY, SONG, ARTIST) VALUES (%s, %s, %s, %s) ON CONFLICT (ID) DO NOTHING").format(
                        sql.Identifier(tableName)),
                        [java_string_hashcode(artistName + extract_song_name(filename)), dirpath, filename, artistName])
                    connection.commit()
                    # print("1 запись успешно вставлена")
                    # Получить результат
                    cursor.execute(sql.SQL("SELECT * from {}").format(sql.Identifier(tableName)))
                    record = cursor.fetchall()
                    # print("Результат", record)
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


def insert_genre_in_table(genre_name, dbName):
    try:
        # Подключиться к существующей базе данных
        connection = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password="password",
                                      host="127.0.0.1",
                                      port="5432",
                                      database=dbName)

        cursor = connection.cursor()

        # перебрать файлы
        # Выполнение SQL-запроса для вставки данных в таблицу
        cursor.execute(sql.SQL(
            "INSERT INTO genres (ID, GENRE) VALUES (%s, %s) ON CONFLICT (ID) DO NOTHING"),
            [java_string_hashcode(genre_name), genre_name])
        connection.commit()
        # print("1 запись успешно вставлена")
        # Получить результат
        cursor.execute(sql.SQL("SELECT * from genres"))
        record = cursor.fetchall()
        # print("Результат", record)
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


def iterateByLines(filename, dbName):
    file = open(filename, 'r')
    for line in file.readlines():
        # print(line[1:get_genre_name(line) + 1])
        tableName = line[1:get_genre_name(line) + 1]
        if len(tableName) != 0:
            insert_genre_in_table(tableName, dbName)
        line = line[get_genre_name(line) + 2:]
        artistName = line[1:get_artist_name(line) + 1]
        print(artistName)
        line = line[get_artist_name(line) + 2:]
        print(line)
        createTableOfSongs(tableName, dbName)
        if line[-1] == '\n':
            line = line[0:-1]
        fill_with_songs(line, tableName, artistName, dbName)
    file.close()


def java_string_hashcode(s):
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000


def get_genre_name(line_from_file):
    i = 0
    i = line_from_file[1:].find('#')
    return i


def get_artist_name(line_from_file):
    i = 0
    i = line_from_file[1:].find('$')
    return i


def extract_song_name(string):
    string = string[4:-4]
    # print(string)
    return string


# main
dbName = "MusonDB"
createTableOfGenres(dbName)
iterateByLines("metainfo.txt", dbName)
