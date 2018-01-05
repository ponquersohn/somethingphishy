#!/usr/bin/env python

import json

class configLoader:
    @staticmethod
    def readConfiguration(configFile):
        with open(configFile) as data_file:
            data = ""
            for line in data_file:
                li = line.strip()
                if not li.startswith("#"):
                    data += "{}\r\n".format(li)
            data = json.loads(data)
        config = dict((k.lower(), v) for k, v in data.iteritems())
        return config
