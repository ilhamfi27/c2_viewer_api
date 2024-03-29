from django.db import connection
from django.conf import settings
import os

db_name = settings.DATABASES["default"]["NAME"]

def operation_backup(session_id):
    queries = []
    try:
        cursor = connection.cursor()

        # db_dump_folder = "sav_backup/"
        namafile = "sav_backup_session_"+str(session_id)+".sql"

        file_path = os.path.join(settings.BASE_DIR, namafile)
        # f = open(namafile, "w")

        query_column = "SELECT * " \
                       "FROM information_schema.columns " \
                       "where table_name = 'sessions' and table_catalog = '{}'" \
                       .format(db_name)
        cursor.execute(query_column)
        rows = cursor.fetchall()
        ar_kolom = []
        ar_tipe_data_kolom = []
        for row in rows:
            print("   " + str(row[3]) + " " + str(row[7]))
            ar_kolom.append(row[3])
            ar_tipe_data_kolom.append(row[7])

        #query data dan bentuk insert statement sql
        query_data = "SELECT * " \
                     "FROM sessions WHERE id = {} " \
                     .format(str(session_id))
        cursor.execute(query_data)
        rows = cursor.fetchall()
        string_sql_insert = "INSERT INTO sessions VALUES ("
        for row in rows:
            # lihat tipe data
            for i in range(len(ar_tipe_data_kolom)):
                if (ar_tipe_data_kolom[i].find('int') > 0):
                    string_sql_insert = string_sql_insert + str(row[i]) + ","
                else:
                    string_sql_insert = string_sql_insert + "'" + str(row[i]) + "',"
        string_sql_insert = string_sql_insert[:-1] + ');\n'
        string_sql_insert = string_sql_insert.replace("'None'", "NULL")
        queries.append(string_sql_insert)
        # f.write(string_sql_insert)

        # mulai seluruh tabel yang berelasi dengan kunci session_id
        sql_tabel_relasi = "SELECT distinct(table_name) " \
                           "FROM information_schema.columns " \
                           "where column_name = 'session_id' " \
                           "and table_name not like '%stored%'"
        cursor.execute(sql_tabel_relasi)
        rows = cursor.fetchall()
        ar_tabel_relasi = []
        for row in rows:
            ar_tabel_relasi.append(str(row[0]))

        for i in range(len(ar_tabel_relasi)):
            # untuk setiap tabel berelasi , cari daftar kolomnya kemudian
            # bentuk sql insertnya
            query_column = "SELECT * " \
                           "FROM information_schema.columns " \
                           "where table_name = '{}' " \
                           "and table_catalog = '{}' " \
                           .format(ar_tabel_relasi[i], db_name)

            cursor.execute(query_column)
            rows = cursor.fetchall()
            ar_kolom = []
            ar_tipe_data_kolom = []
            for row in rows:
                ar_kolom.append(row[3])
                ar_tipe_data_kolom.append(row[7])

            # print(ar_tabel_relasi[i],ar_kolom,ar_tipe_data_kolom)
            # query data dan bentuk insert statement sql
            query_data = "SELECT * " \
                         "FROM {} " \
                         "WHERE session_id = {} " \
                         .format(ar_tabel_relasi[i], str(session_id))
            cursor.execute(query_data)
            rows = cursor.fetchall()

            for row in rows:
                string_sql_insert = "INSERT INTO " + ar_tabel_relasi[i] + "  VALUES ("
                # lihat tipe data
                for ix in range(len(ar_tipe_data_kolom)):
                    if (ar_tipe_data_kolom[ix].find('int') > 0):
                        string_sql_insert = string_sql_insert + str(row[ix]) + ","
                    else:
                        string_sql_insert = string_sql_insert + "'" + str(row[ix]) + "',"
                string_sql_insert = string_sql_insert[:-1] + ');\n'
                string_sql_insert = string_sql_insert.replace("'None'", "NULL")
                string_sql_insert = string_sql_insert.replace("'FALSE'", "FALSE")
                string_sql_insert = string_sql_insert.replace("'TRUE'", "TRUE")
                string_sql_insert = string_sql_insert.replace("[", "{")
                string_sql_insert = string_sql_insert.replace("]", "}")

                queries.append(string_sql_insert)

                # f.write(string_sql_insert)
        # f.close()
        return namafile, queries

    except Exception as error:
        print ("Error while connecting to PostgreSQL", error)
        raise error
    finally:
        #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


def restore_file_handler(f, replaced=False, session_id=None):
    try:
        cursor = connection.cursor()

        if replaced:
            # mulai seluruh tabel yang berelasi dengan kunci session_id
            sql_tabel_relasi = "SELECT distinct(table_name) " \
                               "FROM information_schema.columns " \
                               "where column_name = 'session_id' " \
                               "and table_name not like '%stored%'"
            cursor.execute(sql_tabel_relasi)
            rows = cursor.fetchall()
            for row in rows:
                delete_query = "DELETE FROM " + str(row[0]) + " WHERE session_id = " + session_id
                cursor.execute(delete_query)

            delete_session = "DELETE FROM sessions WHERE id = " + session_id
            cursor.execute(delete_session)

        for chunks in f.readlines():
            chunks = chunks.decode("utf-8")
            cursor.execute(chunks)
    except Exception as error:
        print ("Error while connecting to PostgreSQL", error)
        return False, "Something Went Wrong in The Restore File Query"

    return True, None
