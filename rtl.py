#!/usr/bin/env python3
""" Send LaCrosse Temperature Sensor Data (via software defined radio) to influxdb """

# Copyright (c) 2019  Jay Lubomirski

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import subprocess
import json
import numpy as np
import socket
import sys
import argparse
import time
import pprint
from datetime import datetime

from influxdb import InfluxDBClient

# dewpoint constants (see https://en.wikipedia.org/wiki/Dew_point)
b = 17.27
c = 237.7

def gamma( Tc, RH ):
    """ gamma function takes temperature in celcius and percent relative humidity """
    return ( (np.log(RH/100.0)) + (b * Tc)/(c + Tc) )

def dewpoint ( Tc, RH ):
    """ calculate dewpoint in Celcius given temperature in celcius and relative humidity """
    return ( ( c * gamma(Tc, RH)) / (b - gamma(Tc, RH)) )

def CtoF ( Tc ):
    """ return Temperature in Farenheit """
    return ( (Tc * (9.0/5.0)) + 32.0 )

def get_values():
    """
    Read the sensors available and their values  
    Returns a dictionary with the readings or None on errors
    """

    command_line = ["rtl_433", "-d", "rtl_tcp:192.168.5.20", "-T", "300", "-f", "434078700", "-R", "34", "-F", "json"]
    #command_line = ["cat", "values.txt"]
    humidity = 0
    temperatureC = 0
    foundH = 0
    foundT = 0
    
    p = subprocess.Popen(command_line, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    while foundH == 0 or foundT == 0:
        output = p.stdout.readline()
        if output == '' and p.poll() is not None:
            break
        if output:
            try:
                d = json.loads(output)
                if 'humidity' in d:
                    foundH = 1
                    humidity = float(d['humidity'])
                if 'temperature_C' in d:
                    foundT = 1
                    temperatureC = float(d['temperature_C'])
            except ValueError as e:
                continue
    else:
        p.kill()
    
        dewpointC = dewpoint ( temperatureC, humidity )
        temperatureF = CtoF(temperatureC)
        dewpointF = CtoF(dewpointC)
    
        d = {}
        d['tempC'] = temperatureC
        d['tempF'] = temperatureF
        d['rh'] = humidity
        d['dewC'] = dewpointC
        d['dewF'] = dewpointF
    
        return d
    
    return None

def do_weather_reading( influxclient, timestamp, tags ):

    readings = get_values()
    if readings == None:
        return
    
    print ("Temp: {0}C {1}F, RH: {2}%, Dewpoint: {3}C {4}F".format( readings['tempC'], readings['tempF'], readings['rh'], readings['dewC'], readings['dewF']))

    series = []
    for measurement in ['temperature', 'humidity', 'dewpoint']:
        fields = {}
        if measurement == 'temperature':
            fields["tempF"] = readings['tempF']
            fields["tempC"] = readings['tempC']
        elif measurement == 'humidity':
            fields["rh"] = readings['rh']
        elif measurement == 'dewpoint':
            fields["dewF"] = readings["dewF"]
            fields["dewC"] = readings["dewC"]
                
        d = { "measurement": measurement,
              "tags": tags,
              "time": timestamp,
              "fields": fields
        }
        series.append(d)

    pprint.pprint(series)
    if influxclient:
        influxclient.write_points(series)
    

class rtl:

    def __init__ ( self, rtl_server_ip, rtl_port, electric_id=None, gas_id=None, water_id=None ):
        self.rtl_server = rtl_server_ip
        self.rtl_port = rtl_port
        self.electric_meter_id = electric_id
        self.gas_meter_id = gas_id
        self.water_meter_id = water_id

    def do_water_meter_reading( self, influxclient, timestamp, tags ):
        " read water and send to influx "

        reading = self.get_water_reading()
        if reading == None:
            return
    
        print ("Water: {0} HCC ".format( reading ))

        series = []
        fields = {}
        fields["reading"] = reading
        tags["meterid"] = self.water_meter_id
        tags["units"] = "hcf"
        tags["version"] = 1
        
        d = { "measurement": "water",
              "tags": tags,
              "time": timestamp,
              "fields": fields
        }
        series.append(d)

        pprint.pprint(series)
        if influxclient:
            influxclient.write_points(series)

    def do_gas_electric_meter_reading( self, influxclient, timestamp, tags):
        " read gas & electric and send to influx "

        readings = self.get_gas_and_electric_reading()

        series = []
        
        if "gas" in readings:
            print ("Gas: {0} CCF ".format( readings["gas"] ))

            fields = {}
            fields["reading"] = readings["gas"]
            gas_tags = tags.copy()
            gas_tags["meterid"] = self.gas_meter_id
            gas_tags["units"] = "ccf"
            gas_tags["version"] = 1
                
            d = { "measurement": "gas",
                  "tags": gas_tags,
                  "time": timestamp,
                  "fields": fields
            }
            series.append(d)
        if "electric" in readings:
            print ("Electric: {0} KWH ".format( readings["electric"] ))

            fields = {}
            fields["reading"] = readings["electric"]
            etags = tags.copy()
            etags["meterid"] = self.electric_meter_id
            etags["units"] = "kwh"
            etags["version"] = 1
                        
            d = { "measurement": "electric",
                  "tags": etags,
                  "time": timestamp,
                  "fields": fields
            }
            series.append(d)
        

        pprint.pprint(series)
        if influxclient:
            influxclient.write_points(series)

        
        
    def get_rtl_values( self, msgtype, filterids):
        """
        Read the sensors available and their values  
        Returns a dictionary with the readings or None on errors
        """
        ids = ",".join(filterids)
        server = "{}:{}".format(self.rtl_server, self.rtl_port)

        command_line = ["rtlamr", "-format", "json", "-msgtype", msgtype,
                        "-server", server, "-filterid", ids, "-single", "true"]
        print (" ".join(command_line))
        found = 0
        result = []

        p = subprocess.Popen(command_line, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        while found < len(filterids):
            output = p.stdout.readline()
            print (output)
            if output == '' and p.poll() is not None:
                break
            if output:
                try:
                    d = json.loads(output)
                    result.append(d)
                    found += 1
                except ValueError as e:
                    print ("parse error")
                    continue
        else:
            p.kill()

        return result

    def get_electric_reading( self ):
        " return current reading (in kwh) from meterid "
        if self.electric_meter_id == None:
            return None
        d = self.__get_gas_and_electric_reading(None, self.electric_meter_id)
        return d["electric"]

    
    def get_gas_reading( self ):
        " return current reading (in ccf) from meterid "
        if self.gas_meter_id == None:
            return None
        d = self.__get_gas_and_electric_reading(self.gas_meter_id, None)
        return d["gas"]

    def get_gas_and_electric_reading( self ):
        " return a dictionary with both gas and electric readings "
        return self.__get_gas_and_electric_reading(self.gas_meter_id, self.electric_meter_id)

    
    def __get_gas_and_electric_reading( self, gas_meter_id=None, electric_meter_id=None ):
        " internal call: return dictionary with electric/gas readings "
        gas_ids = []
        elec_ids = []
        if gas_meter_id:
            gas_ids.append(str(gas_meter_id))
        if electric_meter_id:
            elec_ids.append(str(electric_meter_id))
        if (len(gas_ids) + len(elec_ids)) == 0:
            return {}

        if (len(elec_ids)>0):
            le = self.get_rtl_values( "scm", elec_ids )
        if (len(gas_ids) > 0):
            lg = self.get_rtl_values( "scm+", gas_ids )
        l = le + lg
        
        d = {}
        for item in l:
            if gas_meter_id and "EndpointID" in item["Message"] and str(item["Message"]["EndpointID"]) == str(gas_meter_id):
                try:
                    raw = item["Message"]["Consumption"]
                    ccf = float(raw)/100.0
                    d["gas"] = ccf
                except KeyError as e:
                    print ("Parse error, gas reading:", repr(d))
                except ValueError as e:
                    print ("Parse error, gas reading:", repr(d))
            elif electric_meter_id and "ID" in item["Message"] and str(item["Message"]["ID"]) == str(electric_meter_id):
                try:
                    raw = item["Message"]["Consumption"]
                    kwh = float(raw)/100.0
                    d["electric"] = kwh
                except KeyError as e:
                    print ("Parse error, electric reading:", repr(d))
                except ValueError as e:
                    print ("Parse error, electric reading:", repr(d))
        
        return d
    
    def get_water_reading( self ):
        " return current reading (in hcf) from meterid "
        if self.water_meter_id == None:
            return None
        
        d = self.get_rtl_values( "r900bcd", [ str(self.water_meter_id) ] )
        try:
            raw = d[0]["Message"]["Consumption"]
            hcf = float(raw)/100.0
        except IndexError as e:
            print ("No data returned")
            return None
        except KeyError as e:
            print ("Parse error", repr(d))
            return None
        except ValueError as e:
            print ("parse error:", repr(d))
            return None

        return hcf
    


if __name__ == "__main__" :

    parser = argparse.ArgumentParser(description='DHT to influxdb service.')
    parser.add_argument("--influx_server", dest='server', default='localhost', help='influxdb server')
    parser.add_argument("--influx_port", dest='port', default=8086, help='influxdb server port number')
    parser.add_argument("--influx_database", dest='db', default='pi_dht', help='influxdb database name')
    parser.add_argument("--tags", dest='tags', default=None, help='influxdb tags in the form of name=value,name=value')
    parser.add_argument("--interval", dest='interval', default=5, help='interval between readings in minutes')
    parser.add_argument("--rtltcp_ip", dest='rtl_server_ip', default=None, help="ip address of rtltcp server")
    parser.add_argument("--rtltcp_port", dest='rtl_server_port', default=1234, help="port number of rtltcp server")
    parser.add_argument("--water_id", dest='water_id', default=None, help="water meter id number")
    parser.add_argument("--gas_id", dest='gas_id', default=None, help="gas meter id number")
    parser.add_argument("--electric_id", dest='electric_id', default=None, help="electric meter id number")
    parser.add_argument("--meter_db", dest='meter_db', default=None, help="influxdb name for meter readings")
    hostname = socket.gethostname()

    args = parser.parse_args()
    
    meter_client = None
    r = None
    if args.rtl_server_ip:
        r = rtl(args.rtl_server_ip, args.rtl_server_port, args.electric_id, args.gas_id, args.water_id)

        if args.meter_db:
            print ("Connecting to {0}:{1} and writing to database {2}".format(args.server, args.port, args.meter_db))
            meter_client = InfluxDBClient(host=args.server, port=args.port, database=args.meter_db)    

#    weather_client = None
#    print ("Connecting to {0}:{1} and writing to database {2}".format(args.server, args.port, args.db))
#    weather_client = InfluxDBClient(host=args.server, port=args.port, database=args.db)
    
    tags = {'hostname': hostname}
    if args.tags:
        splits = args.tags.split(',')
        for s in splits:
            tag_split = s.split('=')
            tags[tag_split[0]]=tag_split[1]
        print ("Found tags: {0}".format(repr(tags)))

    beat = 0 
    while True:
        t1 = datetime.utcnow()
        stamp = t1.isoformat()

#        do_weather_reading(weather_client, stamp, tags )

        if r:
            r.do_gas_electric_meter_reading(meter_client, stamp, {'hostname': hostname} )
            if (beat * int(args.interval)) % (6 * 60) == 0:
                r.do_water_meter_reading(meter_client, stamp, {'hostname': hostname} )

        t2 = datetime.utcnow()
        td = t2 - t1        
        sleeptime = int(args.interval)*60
        if (sleeptime < td.total_seconds()):
            sleeptime = 0
        else:
            sleeptime -= td.total_seconds()
        print ("Readings took {} seconds, sleeping {} seconds".format(td.total_seconds(), sleeptime))
        time.sleep(sleeptime)
        beat += 1
        
    sys.exit(0)
