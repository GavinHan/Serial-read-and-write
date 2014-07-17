#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
 Created on 2014-4-16
 Author: Gavin_Han
 Email: muyaohan@gmail.com
'''

import threading 
import struct
import serial
import time
import datetime
import MySQLdb

def init():
    config = {}; spot = {}; params = {}; surface_ssll = {}; surface_ljll = {}
    #source_id = 2
    surface_port = {6:8, 3:9, 5:10, 9:11}; surface = {}; cross_sectional = {8:30,9:24,10:51,11:3}
    #source_id = 1
    company_port = {4:3, 7:4, 8:5, 1:6}; company = {}
    #source_id = 3
    underground_port = {2:2, 11:7}; underground = {}

    conn = MySQLdb.connect (host = "127.0.0.1",user = "root",passwd = "1234",db = "water2_production",charset='utf8')
    cursor = conn.cursor ()

    cursor.execute( "SELECT id,name,sensor_device_id,device_param_id,upper_limit,lower_limit FROM sensor_params")
    for row in cursor.fetchall():
        sensor_param_id = int(row[0])
        param_type_name = row[1]
        sensor_device_id = int(row[2])
        device_param_id = int(row[3])
        upper_limit = row[4]
        lower_limit = row[5]
        #gprs_id
        cursor.execute( "SELECT gprs_device_id from sensor_devices where id = {0}".format(sensor_device_id))
        gprs_device_id = int(cursor.fetchall()[0][0])
        #spot_id
        cursor.execute( "select spot_id from gprs_devices where id = {0}".format(gprs_device_id))
        spot_id = int(cursor.fetchall()[0][0])

        #source_id
        cursor.execute( "select source_id from spots where id = {0}".format(spot_id))
        source_id = int(cursor.fetchall()[0][0])

        #device_params
        cursor.execute( "SELECT upper_limit,lower_limit,param_type_id FROM device_params WHERE id = '%d'"%(device_param_id))
        for row in cursor.fetchall():
            upper = row[0]; lower = row[1]; param_type_id = int(row[2])

        cursor.execute( "select unit_name from param_types where id = {0}".format(param_type_id))
        unit_name = cursor.fetchall()[0][0]

        if sensor_param_id not in params.keys():
            params[sensor_param_id] = {}
            params[sensor_param_id]['source_id'] = source_id
            params[sensor_param_id]['spot_id'] = spot_id
            params[sensor_param_id]['gprs_device_id'] = gprs_device_id
            params[sensor_param_id]['sensor_device_id'] = sensor_device_id
            params[sensor_param_id]['param_type_id'] = param_type_id
            params[sensor_param_id]['unit_name'] = unit_name
            
            params[sensor_param_id]['upper_limit'] = upper_limit
            params[sensor_param_id]['lower_limit'] = lower_limit
            params[sensor_param_id]['upper'] = upper
            params[sensor_param_id]['lower'] = lower
        else:
            print 'error'

        if spot_id not in spot.keys():
            spot[spot_id] = [sensor_param_id]
        elif spot_id in spot.keys():
            spot[spot_id].append(sensor_param_id)

        sensor_param_id = int(sensor_param_id)
        if source_id == 2:
            if param_type_id not in surface.keys():
                surface[param_type_id] = [sensor_param_id] 
            else:
                surface[param_type_id].append(sensor_param_id)
        elif source_id == 1:
            if param_type_id not in company.keys():
                company[param_type_id] = [sensor_param_id]
            else:
                company[param_type_id].append(sensor_param_id)
        elif source_id == 3:
            if param_type_id not in underground.keys():
                underground[param_type_id] = [sensor_param_id]
            else:
                underground[param_type_id].append(sensor_param_id)

    for i in range(len(surface_port)):
        config[surface[5][i]] = [surface_port[params[surface[5][i]]['spot_id']],'\x03\x03\x00\x04\x00\x02\x84\x28']
        config[surface[3][i]] = [surface_port[params[surface[3][i]]['spot_id']],'\x02\x03\x00\x07\x00\x01\x35\xF8']
        surface_ssll[surface_port[params[surface[1][i]]['spot_id']]] = surface[1][i]
        surface_ljll[surface_port[params[surface[2][i]]['spot_id']]] = surface[2][i]

    for i in range(len(company_port)):
        config[company[1][i]] = [company_port[params[company[1][i]]['spot_id']],'\x02\x03\x00\x00\x00\x02\xC4\x38']
        config[company[5][i]] = [company_port[params[company[5][i]]['spot_id']],'\x02\x03\x00\x04\x00\x02\x85\xF9']
        config[company[2][i]] = [company_port[params[company[2][i]]['spot_id']],'\x02\x03\x00\x08\x00\x02\x45\xFA']

    for i in range(len(underground_port)):
        config[underground[3][i]] = [underground_port[params[underground[3][i]]['spot_id']],'\x02\x03\x00\x06\x00\x01\x64\x38']
        config[underground[4][i]] = [underground_port[params[underground[4][i]]['spot_id']],'\x02\x03\x00\x07\x00\x01\x35\xF8']
    
    cursor.close()
    conn.close()

    return surface_port, surface_ssll, surface_ljll,cross_sectional, company_port, underground_port, config, spot, params

def insertData(params, sensor_param_id, value, date, conn, sensor_param_one):
    cursor = conn.cursor ()
    lower_limit = params[sensor_param_id]['lower_limit']
    upper_limit = params[sensor_param_id]['upper_limit']
    lower = params[sensor_param_id]['lower']
    upper = params[sensor_param_id]['upper']

    source_id = params[sensor_param_id]['source_id']
    spot_id = params[sensor_param_id]['spot_id']
    gprs_device_id = params[sensor_param_id]['gprs_device_id']
    sensor_device_id = params[sensor_param_id]['sensor_device_id']
    param_type_id = params[sensor_param_id]['param_type_id']
    value_ = value

    print "%s %s %s\n"%(spot_id, sensor_param_id, value_)

    if value_< lower_limit and value_ >lower:
        is_except = unicode('过低','utf8')            
    elif value_ > upper_limit and value_< upper:
        is_except = unicode('过高','utf8')
    elif value_ >upper or value_< lower:
        is_except = unicode('错误','utf8')
    else:
        is_except = unicode('正常','utf8')

    if (value_< lower_limit and value_ >(lower_limit-(lower-lower_limit)*1/4)) or (value_ > upper_limit and value_< (upper_limit+(upper-upper_limit)*1/4)):
        level = 1
    elif (value_ < (lower_limit-(lower-lower_limit)*1/4) and value_ >(lower_limit-(lower-lower_limit)*1/2)) or (value_ > upper_limit+(upper-upper_limit)*1/4) and value_< (upper_limit+(upper-upper_limit)*1/2):
        level = 2
    elif (value_ < (lower_limit-(lower-lower_limit)*1/2) and value_ >(lower_limit-(lower-lower_limit)*3/4)) or (value_ > upper_limit+(upper-upper_limit)*1/2) and value_< (upper_limit+(upper-upper_limit)*3/4):
        level = 3
    elif (value_ < (lower_limit-(lower-lower_limit)*3/4) and value_ >lower) or (value_ > upper_limit+(upper-upper_limit)*3/4) and value_< upper:
        level = 4
    elif value_ < lower or value_ >upper:
        level = 5

    cursor.execute("INSERT INTO history_data VALUES(0,'%s','%f','%d','%s',now(),now(),'%d','%d','%d','%d','%d')"%(date,value_,sensor_param_id,is_except,param_type_id, spot_id,source_id, gprs_device_id, sensor_device_id))
    if is_except == unicode('过低','utf8') or is_except == unicode('过高','utf8'):
        cursor.execute( "SELECT id FROM history_data WHERE gather_time = '%s'"%(date))
        for row in cursor.fetchall():
            history_data_id = row[0]
        cursor.execute("INSERT INTO warns VALUES(0,'%d','%s','%d')"%(history_data_id,date,level))
        conn.commit ()

    if sensor_param_id in sensor_param_one and is_except != unicode('错误','utf8'):
        cursor.execute("DELETE FROM realtime_data WHERE sensor_param_id = '%d'"%(sensor_param_id))
        cursor.execute("INSERT INTO realtime_data VALUES(0,'%s','%f','%d','%s','%d',now(),now(),'%d','%d','%d','%d')"%(date,value_,sensor_param_id,is_except,param_type_id, spot_id, gprs_device_id, sensor_device_id, source_id))
        conn.commit ()
        sensor_param_one.remove(sensor_param_id)

    elif is_except != unicode('错误','utf8'):
        cursor.execute("INSERT INTO realtime_data VALUES(0,'%s','%f','%d','%s','%d',now(),now(),'%d','%d','%d','%d')"%(date,value_,sensor_param_id,is_except,param_type_id, spot_id, gprs_device_id, sensor_device_id, source_id))
        conn.commit ()

def main():
    surface_port, surface_ssll, surface_ljll,cross_sectional, company_port, underground_port, config, spot, params = init()
    num = [0 for i in range(100)]

    class GetData(threading.Thread):
        def __init__(self, spot_id):
            threading.Thread.__init__(self)
            self.spot_id = spot_id
            self.sensor_param = spot[self.spot_id]
            self.time_format="%Y-%m-%d %H:%M:%S"

        def run(self):
            while True:
                date = time.strftime(self.time_format)
                end_time = date
                water_level = {}; water_flow = {}
                for p in surface_port.values():
                    water_level[p] = -1
                    water_flow[p] = -1

                sensor_param_one = []
                for i in range(50):
                    sensor_param_one.append(i)

                for sensor_param_id in self.sensor_param:
                    if sensor_param_id in config.keys():
                        source_id = params[sensor_param_id]['source_id']
                        spot_id = params[sensor_param_id]['spot_id']
                        gprs_device_id = params[sensor_param_id]['gprs_device_id']
                        sensor_device_id = params[sensor_param_id]['sensor_device_id']
                        param_type_id = params[sensor_param_id]['param_type_id']
                        unit_name = params[sensor_param_id]['unit_name']
                        
                        upper_limit = params[sensor_param_id]['upper_limit']
                        lower_limit = params[sensor_param_id]['lower_limit']
                        upper = params[sensor_param_id]['upper'] 
                        lower = params[sensor_param_id]['lower'] 

                        try:
                            port = config[sensor_param_id][0]
                            ser = serial.Serial()
                            ser.port = port
                            ser.baudrate = 9600
                            ser.bytesize = serial.EIGHTBITS
                            ser.parity = serial.PARITY_NONE
                            ser.stopbits = serial.STOPBITS_ONE
                            ser.timeout = 5    
                            ser.open()
                            order = config.get(sensor_param_id)[1]
                            ser.write(order)
                            data = ser.readline()
                            n = len(data)
                            ser.close()

                            conn = MySQLdb.connect (host = "127.0.0.1",user = "root",passwd = "1234",db = "water2_production",charset='utf8')
                            cursor = conn.cursor ()
                            cursor.execute( "SELECT is_func_well from sensor_devices where id = {0}".format(sensor_device_id))
                            is_func_well = cursor.fetchall()[0][0]

                            if n != 0:
                                num[sensor_param_id] = 0

                                if not is_func_well:
                                    cursor.execute( "select DISTINCT(sensor_param_id) from faults where solved = FALSE")
                                    temp_ids = cursor.fetchall()
                                    fault_sensor_ids = []
                                    for row in range(len(temp_ids)):
                                        fault_sensor_ids.append(int(temp_ids[row][0]))
                                    if sensor_param_id in fault_sensor_ids:
                                        fault_sensor_ids.remove(sensor_param_id)
                                        cursor.execute("UPDATE faults SET solved = True, end_time = '%s' WHERE sensor_param_id = '%d'"%(date, sensor_param_id))
                                        conn.commit ()
                                    cursor.execute( "select id from sensor_params where sensor_device_id = {0}".format(sensor_device_id))
                                    temp_ids = cursor.fetchall()
                                    sensor_param_ids = []
                                    for row in range(len(temp_ids)):
                                        sensor_param_ids.append(int(temp_ids[row][0]))
                                    flag = True
                                    for i in sensor_param_ids:
                                        if i in fault_sensor_ids:
                                            flag = False
                                            break
                                    if flag == True:
                                        cursor.execute("UPDATE sensor_devices SET is_func_well = True WHERE id = '%d'"%(sensor_device_id))
                                        conn.commit ()

                                if (port in underground_port.values()) and (data[:3].encode('hex') == '020302'):
                                    temp = data[3:5].encode('hex')
                                    value = (upper-lower)/1600 * (int(temp,16)-400)+lower
                                    value_ = float("%.2f"%value)

                                elif (port in surface_port.values()) and (data[:3].encode('hex') == '020302'):
                                    temp = data[3:5].encode('hex')
                                    value = (upper-lower)/1600 * (int(temp,16)-400)+lower
                                    if value < 0:
                                        value_ = abs(float("%.2f"%value))
                                    else:
                                        value_ = float("%.2f"%value)
                                    water_level[port] = value_

                                elif (port in surface_port.values()) and (data[:3].encode('hex') == '030304'):
                                    temp = data[3:7].encode('hex')
                                    temp1 = data[3:5]
                                    temp2 = data[5:7]
                                    s = temp2 + temp1
                                    s = s.encode('hex')
                                    value = struct.unpack('!f',s.decode('hex'))[0]
                                    if value < 0:
                                        value_ = abs(float("%.2f"%value))
                                    else:
                                        value_ = float("%.2f"%value) 
                                    water_flow[port] = value_ 

                                elif port in company_port.values() and data[:3].encode('hex') == '020304':
                                    temp = data[3:7].encode('hex')
                                    temp1 = data[3:5]
                                    temp2 = data[5:7]
                                    s = temp2 + temp1
                                    s = s.encode('hex')

                                    if param_type_id == 5:
                                        value = struct.unpack('!f',s.decode('hex'))[0]
                                        value_ = float("%.2f"%value)

                                    if param_type_id == 1:
                                        value = struct.unpack('!f',s.decode('hex'))[0]/3600.0 
                                        value_ = float("%.2f"%value)                            

                                    elif param_type_id == 2:
                                        value_ = int(s,16)

                                insertData(params, sensor_param_id, value_, date, conn, sensor_param_one)
                                if port in surface_port.values() and (water_flow[port] >=0 and water_level[port] >= 0):
                                    surface = {}
                                    sensor_param_1 = surface_ssll[port] 
                                    sensor_param_2 = surface_ljll[port]
                                    value = water_flow[port]*water_level[port]*cross_sectional[port]
                                    value_ssll = float("%.2f"%value)
                                    surface[sensor_param_1] = value_ssll
                                    try:
                                        cursor.execute( "SELECT substring_index(max(concat(gather_time, '-', value)),'-',3) as pre_time,substring_index(max(concat(gather_time, '-', value)),'-',-1) as value FROM history_data WHERE sensor_param_id = '%d' and `status` <> '错误'"%(sensor_param_2))
                                        for row in cursor.fetchall():
                                            pre_time = row[0]
                                            value_ljll = row[1]
                                        conn.commit ()
                                    except Exception, e:
                                       print e
                                    if pre_time != None and value_ljll != None:
                                        pre_time = pre_time
                                        value_ljll = value_ljll
                                    else:
                                        print "ljll is empty!"
                                        pre_time = date
                                        value_ljll = value_ssll
                                    pre_time = pre_time.encode('utf-8')
                                    if type(end_time) == datetime.datetime:
                                        pass
                                    else: 
                                        end_time = datetime.datetime.strptime(end_time,self.time_format)
                                    pre_time = datetime.datetime.strptime(pre_time,self.time_format)
                                    time_quantum = (end_time - pre_time).seconds
                                    value_ljll = float(value_ljll)

                                    value_ljll += value * time_quantum  
                                    value_ljll = float("%.2f"%value_ljll)
                                    surface[sensor_param_2] = value_ljll

                                    for sensor_param_id in surface.keys():
                                        value_ = surface[sensor_param_id]
                                        insertData(params, sensor_param_id, value_, date, conn, sensor_param_one)

                            elif n == 0 and is_func_well:
                                num[sensor_param_id] += 1
                                if num[sensor_param_id] >= 10:
                                    num[sensor_param_id] = 0
                                    cursor.execute("UPDATE sensor_devices SET is_func_well = FALSE WHERE id = '%d'"%(sensor_device_id))
                                    cursor.execute("INSERT INTO faults VALUES(0,'%d','连续十次没有返回值','%s',null,null,null,null,0)"%(sensor_param_id,date))
                                    conn.commit ()
                            else:
                                continue

                            cursor.close()
                            conn.close()
                        except Exception,ex:
                            import traceback  
                            print traceback.print_exc()
                time.sleep(90)

    threads = []
    for spot_id in spot.keys():
        threads.append(GetData(spot_id))
    for t in threads:
        t.start()

    for t in threads:
        t.join()


if __name__ == '__main__':
    main()
