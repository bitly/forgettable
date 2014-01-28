import numpy as np
import logging
import time
import redis

r = redis.StrictRedis(
    'localhost',
    port=6379,
    db=2
)

def interleave_izip(*iterables):
    # interleave_izip('ABCD', 'xy') --> A x B y
    iterators = map(iter, iterables)
    while iterators:
        for i in iterators:
            yield i.next()

class Distribution(object):
    def __init__(self,k):
        self.k = k

    def decay(self, rate=0.02):
        """
        returns the amount to decay each bin by
        """
        t = int(time.time())
        tau = t-self.last_updated
        rates = [v * rate * tau for v in self.values]
        y = np.random.poisson(rates)
        return y,t

    def incr(self,bin):
        """
        on an event, update the sorted set and the normalizing constant
        """
        r.zincrby(self.k, bin)
        a = r.incr(self.k+"_z")
        if a == 1:
            # this catches the situtation where we've never seen the 
            # the key before, setting t to the time of the initial write
            r.set(self.k+'_t', int(time.time()))

    def __str__(self):
        return str(dict(zip(self.keys,self.values)))

    def decrement(self):
        # check this distribution exists to decrement
        if not r.exists(self.k):
            raise KeyError('Cannot find distribution in Redis')
        # get the currently stored data
        self.keys, self.values = zip(*r.zrevrange(self.k,0,-1,withscores=True))
        self.z = r.get(self.k+"_z") 
        self.n = len(self.values)
        self.last_updated = int(r.get(self.k+"_t"))
        # get the amount to decay by
        y,t = self.decay()
        # decay values by y
        self.values -= y
        self.values[self.values <= 0] = 1
        # normalizing constant
        self.z = int(self.values.sum())
        # build multi call
        pipeline = r.pipeline()
        pipeline.watch(self.k, self.k+'_t', self.k+'_z')
        pipeline.multi()
        pipeline.zadd(self.k, *interleave_izip(self.values, self.keys)) 
        pipeline.set(self.k+'_t', t)
        pipeline.set(self.k+'_z', self.z)
        try:
            # try to excute
            pipeline.execute()
        except redis.WatchError:
            pass

    def get_dist(self):
        self.decrement()
        normalised = dict([(k, v/self.z) for k,v in zip(self.keys, self.values)])
        return normalised

    def get_bin(self, bin):
        self.decrement()
        try:
            out = self.values[self.keys.index(bin)] / self.z
        except ValueError:
            raise ValueError('bin not in distribution')
        return out

