import collections
import json
import sys
import argparse
from kafka import KafkaConsumer, KafkaProducer
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import urlparse
import cgi
from kazoo.client import KazooClient
import logging
import socket
import uuid
import EnginesHashRing
import random

logging.basicConfig()

class PersistentDict(collections.MutableMapping):
    def __init__(self, fileName):
        self.store = dict()
        self._fileName=fileName
        self._readFromfile()
        
    def __del__(self):
        self._writeTofile()
        
    def __getitem__(self, key):
        return self.store[self.__keytransform__(key)]

    def __setitem__(self, key, value):
        self.store[self.__keytransform__(key)] = value
        

    def __delitem__(self, key):
        del self.store[self.__keytransform__(key)]

    def __iter__(self):
        return iter(self.store)

    def __len__(self):
        return len(self.store)

    def __keytransform__(self, key):
        return key
    
    def _readFromfile(self):
        try:
            with open(self._fileName, 'r') as fp:
                self.store = json.load(fp)
        except:
            print('File not found %s' % self._fileName)
     
    def _writeTofile(self):
        with open(self._fileName, 'w') as fp:
            json.dump(self.store, fp)    

class EngineSimulator:
    def __init__(self, nodeid, port=8000):
        self._nodeid=nodeid
        self._workTopics=[]
        self._ipAddress=("%s:%d"%(socket.gethostbyname(socket.gethostname()), port))
        self._nodeName=("engine_%d" % self._nodeid)

        # cache DBs
        self._localDbs={}
        # consumer
        self._consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                                       auto_offset_reset='earliest',
                                       consumer_timeout_ms=502,
                                       request_timeout_ms=501)
        # producer
        self._producer = KafkaProducer(bootstrap_servers='localhost:9092')
        # zookeeper client
        self._zk = KazooClient(hosts='127.0.0.1:2181')
        self._zk.start()
        
        # init engines hash ring
        self._enginesHashRing=EnginesHashRing.EnginesHashRing([])

        # set watcher for new engines
        @self._zk.ChildrenWatch("/Engines/Ready")
        def watch_children(children):
            print("Engines are now: %s" % children)
            engines=[]
            for engineName in children:
                data, stat = self._zk.get("/Engines/Ready/%s"%engineName)
                print("Name: %s, Version: %s, data: %s" % (engineName, stat.version, data.decode("utf-8")))
                engines.append(data)
            self._enginesHashRing=EnginesHashRing.EnginesHashRing(engines)
            
            # update work topics
            self._workTopics=self._enginesHashRing.workTopics2engine(self._ipAddress)
            backupTopics=self._enginesHashRing.backupTopics2engine(self._ipAddress)

            print('backup topics [%d]:' % len(backupTopics))
            print(backupTopics)
            print('work topics [%d]:' % len(self._workTopics))
            print(self._workTopics)

            # subscribe to new kafka topics
            allTopics=self._workTopics + backupTopics
            if len(allTopics) > 0:
                self._consumer.subscribe(allTopics)

            for topic in self._workTopics:
                if topic not in self._localDbs:
                    self._localDbs[topic]=PersistentDict("%s_%s.json" % (self._ipAddress, topic))
    
    def __del__(self):
        self._producer.close()
        self._consumer.close()
    
#         for db in self._localDbs:
#             self._localDbs[db].writeTofile()

    def loadTopics(self):
        # build local DB from topics
        if len(self._workTopics) > 0:
            for msg in self._consumer:
                act = json.loads(msg.value)
                self._localDbs[msg.topic][act['user']]=msg
#                 print(('READ:    %s: %s' % (msg.topic, msg.value)))

    def zkRegister(self):
        self._zk.ensure_path("/Engines/Registered")
        self._zk.create(("/Engines/Registered/%s" % self._nodeName) , value=self._ipAddress, ephemeral=True)
#         zk.delete(("/Engines/engine_%d" % self._nodeid))
#         if zk.exists("/Engines"):
#             data, stat = zk.get("/Engines")
#             print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
#             children = zk.get_children("/Engines")
#             print("There are %s children with names %s" % (len(children), children))
#             for znode in children:
#                 data, stat = zk.get("/Engines/%s"%znode)
#                 print("Name: %s, Version: %s, data: %s" % (znode, stat.version, data.decode("utf-8")))

    def zkReady(self):
        self._zk.ensure_path("/Engines/Ready")
        self._zk.create(("/Engines/Ready/%s" % self._nodeName) , value=self._ipAddress, ephemeral=True)

    def _checkActivityUserSeq(self, newActivity, topic):
        pervSeq=0
        if len(self._localDbs[topic]) > 0:
            if newActivity['user'] in self._localDbs[topic]:
                rec=self._localDbs[topic][newActivity['user']]
                dictRec=json.loads(rec)
                pervSeq = dictRec['userSequence']
        newSeq = newActivity['userSequence']
        if newSeq == pervSeq + 1:
            print('Activity userSequence MATCH %d' % (newSeq))
            return True
        else:
            print('Activity userSequence ERROR %d. prev seq:%d last seq: %d' % (pervSeq-newSeq, pervSeq, newSeq))
            return False
        
    def ProcessActivity(self, activity):
            topic=EnginesHashRing.user2topic(activity['user'])
            self._checkActivityUserSeq(activity, topic)
            self._updateCache()
            self._persistCache()

            jsonActivity=json.dumps(activity)
            self._producer.send(topic, jsonActivity)
            print('WRITE:    %s: %s' % (topic, activity))
    
    def _persistCache(self):
        if random.randint(0, 10) == 1:
            for db in self._localDbs:
                self._localDbs[db]._writeTofile()
        
    def _updateCache(self):
        pollList = self._consumer.poll(timeout_ms=0, max_records=100)
        for tp in pollList:
            for msg in pollList[tp]:
                dictActivity=json.loads(msg.value)
                if tp.topic in self._localDbs:
                    self._localDbs[tp.topic][dictActivity['user']] = msg.value
#                 print(('READ:    %s: %s' % (tp.topic, msg.value)))

def MakeHandlerClassWithParams(engineSimulator):
    class EngineHttpReqHandler(BaseHTTPRequestHandler, object):
        def __init__(self, *args, **kwargs):
            self.timeout=20
            self._engineSimulator=engineSimulator
            super(EngineHttpReqHandler, self).__init__(*args, **kwargs)
        def _set_headers(self):
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
    
        def do_GET(self):
            self._set_headers()
            parsed_path = urlparse.urlparse(self.path)
            request_id = parsed_path.path
            response = 'GET response\n'
            self.wfile.write(json.dumps(response))
    
        def do_POST(self):
            self._set_headers()
            ctype, pdict = cgi.parse_header(self.headers.getheader('content-type'))
            if ctype == 'multipart/form-data':
                postvars = cgi.parse_multipart(self.rfile, pdict)
            elif ctype == 'application/x-www-form-urlencoded':
                length = int(self.headers.getheader('content-length'))
                postvars = cgi.parse_qs(self.rfile.read(length), keep_blank_values=1)
                #activity=json.loads('{"a":"aa", "b":"bb"}')
                activityStr=postvars['activity'][0]
                activityDict=json.loads(activityStr)
                self._engineSimulator.ProcessActivity(activityDict)
            else:
                postvars = {}
            response = 'POST response'
#             self.wfile.write(json.dumps(response))
    
        def do_HEAD(self):
            self._set_headers()
        def log_message(self, format, *args):
            return
        
    return EngineHttpReqHandler

def run(engineSimulator, server_class=HTTPServer, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, MakeHandlerClassWithParams(engineSimulator))
    print 'Starting httpd...'
    httpd.serve_forever()

if __name__ == "__main__":
    # parse arguments
    argParser = argparse.ArgumentParser(description='Engine Simulator')
    argParser.add_argument('-p', '--port', help='port', default=8000, type=int)    
    args = argParser.parse_args(sys.argv[1:])
    
    # create engine simulator and load topics from kafka
    engineSim=EngineSimulator(uuid.uuid4(), args.port)
    engineSim.zkRegister()
    engineSim.loadTopics()
    engineSim.zkReady()
    
    # start http server
    run(engineSimulator=engineSim, port=args.port)

    del engineSim
