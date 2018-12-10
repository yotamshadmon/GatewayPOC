from uhashring import HashRing
import uuid
from _bisect import bisect
from multiprocessing.pool import worker

NUMBER_OF_TOPICS=10

def user2topic(user):
    userHash=int(uuid.uuid5(uuid.NAMESPACE_DNS, user.encode('utf-8')).get_node())
    topic=userHash%NUMBER_OF_TOPICS
    return ('t%d' % topic)
    
class EnginesHashRing(HashRing):
    def __init__(self, nodes=None, **kwargs):
        HashRing.__init__(self, nodes, **kwargs)
        
    def topic2engine(self, topic):
        topicStr=('t%d' % topic)
        return self.get_node(key=topicStr) 

    def user2engine(self, user):
        topic=user2topic(user)
        engineAddress = self.get_node(key=topic)
        return engineAddress
    
    def user2backupEngine(self, user):
        topic=user2topic(user)
        pos = self._get_pos(key=topic)
        engineAddress = self._get_next_node(pos)
        return engineAddress

    def workTopics2engine(self, engineIP):
        workTopics=[]
        for t in range(0, NUMBER_OF_TOPICS):
            if engineIP == self.topic2engine(t):
                topicName=('t%d' % t)
                workTopics.append(topicName)
        return workTopics
    
    def _get_next_node(self, pos):
        node=self.runtime._ring[self.runtime._keys[pos]]
        nextNode=node
        npos=pos+1
        while nextNode == node and npos != pos:
            if npos >= len(self.runtime._keys):
                npos = 0
            nextNode=self.runtime._ring[self.runtime._keys[npos]]
            npos = npos+1
            
        if nextNode != node:
            return nextNode
        else:
            return None
        
    def backupTopics2engine(self, engineIP):
        backupTopics=[]
        if len(self.runtime._keys) > 0:
            for t in range(0, NUMBER_OF_TOPICS):
                topicName=('t%d' % t)
                pos=self._get_pos(topicName)
                engine = self.runtime._ring[self.runtime._keys[pos]]
                if engine != engineIP:
                    if self._get_next_node(pos) == engineIP:
                        backupTopics.append(topicName)
        return backupTopics
