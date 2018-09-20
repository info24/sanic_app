filename="phone.csv"

fp = open(filename)

tmp_guid = ''
tmp_room = ''
tmp_time = ''

while 1:
    data = fp.readline()
    if data:
        data = data.split()
        if tmp_room == data[2]:
            print(tmp_time, tmp_guid, tmp_room)


        tmp_guid = data[1]
        tmp_time = data[0]
        tmp_room = data[2]
    else:
        break


fp.close()


