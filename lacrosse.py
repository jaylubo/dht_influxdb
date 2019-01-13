#!/usr/bin/python
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

    command_line = ["rtl_433", "-f", "434078700", "-R", "34", "-F", "json"]
    #command_line = ["cat", "values.txt"]
    humidity = 0
    temperatureC = 0
    foundH = 0
    foundT = 0
    
    p = subprocess.Popen(command_line, stdout=subprocess.PIPE)
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



if __name__ == "__main__" :

    parser = argparse.ArgumentParser(description='DHT to influxdb service.')
    parser.add_argument("--influx_server", dest='server', default='localhost', help='influxdb server')
    parser.add_argument("--influx_port", dest='port', default=8086, help='influxdb server port number')
    parser.add_argument("--influx_database", dest='db', default='pi_dht', help='influxdb database name')
    parser.add_argument("--tags", dest='tags', default=None, help='influxdb tags in the form of name=value,name=value')
    parser.add_argument("--interval", dest='interval', default=5, help='interval between readings in minutes')
    hostname = socket.gethostname()

    args = parser.parse_args()

    print "Connecting to {0}:{1} and writing to database {2}".format(args.server, args.port, args.db)
    client = InfluxDBClient(host=args.server, port=args.port, database=args.db)

    tags = {'hostname': hostname}
    if args.tags:
        splits = args.tags.split(',')
        for s in splits:
            tag_split = s.split('=')
            tags[tag_split[0]]=tag_split[1]
        print "Found tags: {0}".format(repr(tags))
    
    while True:
        stamp = datetime.utcnow().isoformat()

        readings = get_values()
        if readings == None:
            time.sleep(int(args.interval)*60)
            continue

        print "Temp: {0}C {1}F, RH: {2}%, Dewpoint: {3}C {4}F".format( readings['tempC'], readings['tempF'], readings['rh'], readings['dewC'], readings['dewF'])

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
                  "time": stamp,
                  "fields": fields
                  }
            series.append(d)

        print repr(series)
        client.write_points(series)
        time.sleep(int(args.interval)*60)

        
    sys.exit(0)
