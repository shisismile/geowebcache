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
public class MongoBlobStoreInfo extends BlobStoreInfo {
    /**
     * example:
     * mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
     */
    private String mongoUrl;

    /**
     * x filed name
     */
    private String xTile;
    /** y filed name */
    private String yTile;
    /** z filed name */
    private String zoom;
    /** img filed name ;should binary type */
    private String img;

    public String getMongoUrl() {
        return mongoUrl;
    }

    public void setMongoUrl(String mongoUrl) {
        this.mongoUrl = mongoUrl;
    }

    public String getxTile() {
        return xTile;
    }

    public void setxTile(String xTile) {
        this.xTile = xTile;
    }

    public String getyTile() {
        return yTile;
    }

    public void setyTile(String yTile) {
        this.yTile = yTile;
    }

    public String getZoom() {
        return zoom;
    }

    public void setZoom(String zoom) {
        this.zoom = zoom;
    }

    public String getImg() {
        return img;
    }

    public void setImg(String img) {
        this.img = img;
    }

    @Override
    public String toString() {
        return "MongoInfo BlobStore";
    }

    @Override
    public BlobStore createInstance(TileLayerDispatcher layers, LockProvider lockProvider)
            throws StorageException {
        return new MongoBlobStore(this);
    }

    @Override
    public String getLocation() {
        return mongoUrl;
    }
}
