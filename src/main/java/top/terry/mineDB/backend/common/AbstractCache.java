package top.terry.mineDB.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.terry.mineDB.common.Error;

/**
 * AbstractCache: cache implemented a reference counting strategy
 */
public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache;                     // actual cached data
    private HashMap<Long, Integer> references;          // references for an element
    private HashMap<Long, Boolean> getting;             // thread requiring resource

    private int maxResource;                            // max cache resource
    private int count = 0;                              // elements in
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception {
        while(true) {
            lock.lock();
            if(getting.containsKey(key)) {
                // other threads are requiring the resource
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if(cache.containsKey(key)) {
                // resource in cache, direct return
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // try to get resource
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            count ++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        T obj = null;
        try {
            obj = getForCache(key);
        } catch(Exception e) {
            lock.lock();
            count --;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();
        
        return obj;
    }

    /**
     * force release a cache
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key)-1;
            if(ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count --;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * close cache, release all resource
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * get when resource is not in cache
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * release when resource is evicted
     */
    protected abstract void releaseForCache(T obj);
}
