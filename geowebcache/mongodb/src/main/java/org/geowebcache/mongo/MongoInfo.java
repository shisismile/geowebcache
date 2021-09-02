package org.geowebcache.mongo;

import org.geowebcache.config.BlobStoreInfo;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.locks.LockProvider;
import org.geowebcache.storage.BlobStore;
import org.geowebcache.storage.StorageException;


/**
 * @author shimingen
 * @date 2021/8/27
 */
public class MongoInfo extends BlobStoreInfo {
    /**
     * example:  mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
     */
    private String mongoUrl;


    public String getMongoUrl() {
        return mongoUrl;

    }

    public void setMongoUrl(String mongoUrl) {
        this.mongoUrl = mongoUrl;
    }

    @Override
    public String toString() {
        return "MongoInfo BlobStore";
    }

    @Override
    public BlobStore createInstance(TileLayerDispatcher layers, LockProvider lockProvider) throws StorageException {
        return null;
    }

    @Override
    public String getLocation() {
        return mongoUrl;
    }
}
