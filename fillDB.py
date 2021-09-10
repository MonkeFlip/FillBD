import os
import psycopg2
from psycopg2 import Error
from psycopg2 import sql


def createTableOfSongs(dbName, psw):
    try:
        # Подключиться к существующей базе данных
        connection = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password=psw,
                                      host="127.0.0.1",
                                      port="5432",
                                      database=dbName)

        cursor = connection.cursor()

        # Выполнение команды: это создает новую таблицу
        cursor.execute(sql.SQL(
            "CREATE TABLE IF NOT EXISTS SONGS (ID INT PRIMARY KEY NOT NULL, DIRECTORY VARCHAR NOT NULL, SONG VARCHAR "
            "NOT NULL, ARTIST VARCHAR NOT NULL, GENRE VARCHAR NOT NULL)"))
        connection.commit()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def createTableOfGenres(dbName, psw):
    try:
        # Подключиться к существующей базе данных
        connection = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password=psw,
                                      host="127.0.0.1",
                                      port="5432",
                                      database=dbName)

        cursor = connection.cursor()

        # Выполнение команды: это создает новую таблицу
        cursor.execute("CREATE TABLE IF NOT EXISTS genres (ID INT PRIMARY KEY NOT NULL, GENRE VARCHAR NOT NULL)")
        connection.commit()
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def fill_with_songs(directory_name, genre_name, artistName, dbName, psw):
    try:
        # Подключиться к существующей базе данных
        connection = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password=psw,
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
                        "INSERT INTO SONGS (ID, DIRECTORY, SONG, ARTIST, GENRE) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (ID) DO "
                        "NOTHING"),
                        [java_string_hashcode(artistName + extract_song_name(filename)), dirpath, filename, artistName, genre_name])
                    connection.commit()
                    # Получить результат
                    cursor.execute(sql.SQL("SELECT * from SONGS"))
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def insert_genre_in_table(genre_name, dbName, psw):
    try:
        # Подключиться к существующей базе данных
        connection = psycopg2.connect(user="postgres",
                                      # пароль, который указали при установке PostgreSQL
                                      password=psw,
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
        # Получить результат
        cursor.execute(sql.SQL("SELECT * from genres"))
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()



def iterateByLines(filename, dbName, psw):
    file = open(filename, 'r')
    for line in file.readlines():
        genre_name = line[1:get_genre_name(line) + 1]
        if len(genre_name) != 0:
            insert_genre_in_table(genre_name, dbName, psw)
        line = line[get_genre_name(line) + 2:]
        artistName = line[1:get_artist_name(line) + 1]
        print(artistName)
        line = line[get_artist_name(line) + 2:]
        print(line)
        createTableOfSongs(dbName, psw)
        if line[-1] == '\n':
            line = line[0:-1]
        fill_with_songs(line, genre_name, artistName, dbName, psw)
    file.close()


def java_string_hashcode(s):
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000


def get_genre_name(line_from_file):
    i = line_from_file[1:].find('#')
    return i


def get_artist_name(line_from_file):
    i = line_from_file[1:].find('$')
    return i


def extract_song_name(string):
    string = string[4:-4]
    return string


# main
dbName = "MusonDB"
print("Please enter the password for the user 'postgres':")
psw = input()
createTableOfGenres(dbName,psw)
iterateByLines("metainfo.txt", dbName,psw)
print("\nBD filled successfully.\n")