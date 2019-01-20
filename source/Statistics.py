import time

class Cadence:
    def __init__(self, text):
        self._text = text
        self._start = int(round(time.time() * 1000))
        self._count = 0
        self._lastTime = self._start
        self._lastCount = 0
        
    def text(self):
        return self._text
    
    def inc(self):
        self._count = self._count + 1
        self._lastCount = self._lastCount + 1
        return self._count
        
    def _getCadence(self, start, count):
        if count == 0:
            return 0, 0, 0
        
        now = int(round(time.time() * 1000))
        secElapsed = ((now - start) / 1000)

        if secElapsed > 0:
            return secElapsed, count, count / secElapsed
        else:
            return 0, 0, 0

    def getCadence(self):
        return self._getCadence(self._start, self._count)

    def getLastCadence(self):
        sec, count, cadence = self._getCadence(self._lastTime, self._lastCount)
        self._lastTime = int(round(time.time() * 1000))
        self._lastCount = 0
        return sec, count, cadence
    
    