import numpy as np
import time
import datetime
from actions.user import send_message, send_notification

async def data_processing(important_data, STATE, USERS, NON_REALTIME_USERS,
                             data_category='', mandatory_attr='', must_remove=[], debug=True):
    # cek apakah datanya berisi?
    if len(important_data) > 0:
        # variable existing data digunakan untuk mengecek apakah datanya baru atau data lama
        existing_data = STATE['existed_data'] + STATE['removed_data']

        if debug:
            print(data_category + ' existing ', existing_data)
            print(data_category + ' = clean existing data', STATE['existed_data'])
            print(data_category + ' = removed data', STATE['removed_data'])

        # mengambil data selisih dari variable yang sudah ada dan data yang baru
        check_track_number_system = np.setdiff1d(important_data[0:, 0], np.array(existing_data))

        if debug:
            print(data_category + ' new track length', len(check_track_number_system))
            print(data_category + ' new track_number_system', check_track_number_system)

        # cek apakah datanya lebih dari 0
        if len(check_track_number_system) > 0:

            # jika masuk ke kondisi ini, maka ada data yang baru dari database
            new_datas = []
            for i, new_data in enumerate(important_data):

                # jika data yang diterima statusnya deleted, maka akan dikondisikan ke variable delete
                if new_data[1][mandatory_attr] in must_remove and \
                    new_data[0] not in STATE['removed_data']:
                    STATE['removed_data'].append(new_data[0])
                else:
                    # jika data tidak di memori existed data dan removed data
                    # maka akan membuat data baru dan dikirim ke user
                    if new_data[0] not in STATE['existed_data'] and \
                        new_data[0] not in STATE['removed_data']:
                        STATE['existed_data'].append(new_data[0])

                        # struktur cached data adalah array of 2d arrays
                        STATE['cached_data'].append(new_data)
                        new_datas.append(new_data[1])
            await send_message(USERS, new_datas ,data_category)
            await send_notification(NON_REALTIME_USERS, new_datas ,data_category)

        # jika data tidak ada perubahan jumlah
        if len(check_track_number_system) == 0:
            changed_data = []

            # mengecek apakah datanya terdapat pada memori remove ?
            # jika iya maka data akan di proses dan dikirim sebagai data baru
            for i, data in enumerate(important_data):
                if data[0] in STATE['removed_data'] and \
                    important_data[i, 1][mandatory_attr] not in must_remove:
                    STATE['existed_data'].append(data[0])
                    STATE['cached_data'].append(data)
                    STATE['removed_data'].remove(data[0])
                    changed_data.append(data)
                    if debug:
                        print(data_category + ' brand new')

            if len(STATE['cached_data']) > 0:
                for i, data in enumerate(STATE['cached_data']):

                    # cari data dari important data dimana id nya sama dengan id yang ada
                    # di cached data
                    important_data_idx = np.where(important_data[:, 0] == data[0])

                    # kalau datanya ada
                    if len(important_data_idx[0]) > 0:

                        # jika data tidak sama dengan data yang baru,
                        # int(important_data_idx[0]) isinya 1 angka hasil dari pencarian index di atas
                        if data[1] != important_data[int(important_data_idx[0]), 1]:

                            # maka akan di proses dengan pengecekan apakah statusnya delete atau bukan
                            if important_data[int(important_data_idx[0]), 1][mandatory_attr] in must_remove:

                                # status delete akan mengganti data dalam memori removed data dan existed data dan
                                # menghapus data dari memori existed
                                STATE['removed_data'].append(data[0])
                                STATE['existed_data'].remove(data[0])
                                STATE['cached_data'].pop(i)

                                # data akan dikirim ke user
                                changed_data.append(important_data[int(important_data_idx[0]), :])
                                if debug:
                                    print(data_category + ' deleted')
                            else:
                                # jika status data selain deleted maka memori cached data akan direplace dengan data baru
                                # dan dikirim ke user
                                changed_data.append(data)
                                STATE['cached_data'][i][1] = important_data[int(important_data_idx[0]), 1]
                                if debug:
                                    print(data_category + ' updated')
                    else:
                        if debug:
                            print('can\'t find data', data[0])
            if debug:
                print(data_category + ' track changed', changed_data)
            changed = np.array(changed_data)
            if len(changed_data) > 0:
                await send_message(USERS, list(changed[0:, 1]) ,data_category)
                await send_notification(NON_REALTIME_USERS, list(changed[0:, 1]) ,data_category)

    if debug:
        print(data_category + ' \n==', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), '\n\n')

async def non_strict_data_processing(important_data, STATE, USERS, NON_REALTIME_USERS, data_category='', debug=True):
    # cek apakah datanya berisi?
    if len(important_data) > 0:
        # variable existing data digunakan untuk mengecek apakah datanya baru atau data lama
        existing_data = STATE['existed_data']

        if debug:
            print(data_category + ' existing ', existing_data)

        # mengambil data selisih dari variable yang sudah ada dan data yang baru
        check_track_number_system = np.setdiff1d(important_data[0:, 0], np.array(existing_data))

        if debug:
            print(data_category + ' new track length', len(check_track_number_system))
            print(data_category + ' new track_number_system', check_track_number_system)

        # cek apakah datanya lebih dari 0
        if len(check_track_number_system) > 0:

            # jika masuk ke kondisi ini, maka ada data yang baru dari database
            new_datas = []
            for i, new_data in enumerate(important_data):

                # struktur cached data adalah array of 2d arrays
                if new_data[0] not in STATE['existed_data']:
                    STATE['existed_data'].append(new_data[0])
                    STATE['cached_data'].append(new_data)
                    new_datas.append(new_data[1])
            await send_message(USERS, new_datas ,data_category)
            await send_notification(NON_REALTIME_USERS, new_datas ,data_category)

        # jika data tidak ada perubahan jumlah
        if len(check_track_number_system) == 0:
            changed_data = []

            if len(STATE['cached_data']) > 0:
                for i, data in enumerate(STATE['cached_data']):

                    # cari data dari important data dimana id nya sama dengan id yang ada
                    # di cached data
                    important_data_idx = np.where(important_data[:, 0] == data[0])

                    # kalau datanya ada
                    if len(important_data_idx[0]) > 0:

                        # jika data tidak sama dengan data yang baru,
                        # int(important_data_idx[0]) isinya 1 angka hasil dari pencarian index di atas
                        if data[1] != important_data[int(important_data_idx[0]), 1]:
                            changed_data.append(data)
                            STATE['cached_data'][i][1] = important_data[int(important_data_idx[0]), 1]
                            if debug:
                                print(data_category + ' updated')
                    else:
                        if debug:
                            print('can\'t find data', data[0])
            if debug:
                print(data_category + ' track changed', changed_data)
            changed = np.array(changed_data)
            if len(changed_data) > 0:
                await send_message(USERS, list(changed[0:, 1]) ,data_category)
                await send_notification(NON_REALTIME_USERS, list(changed[0:, 1]) ,data_category)

    if debug:
        print(data_category + ' \n==', datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), '\n\n')
