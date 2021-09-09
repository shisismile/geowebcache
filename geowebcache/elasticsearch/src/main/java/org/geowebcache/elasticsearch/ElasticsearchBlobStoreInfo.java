package org.geowebcache.elasticsearch;

import org.apache.http.HttpHost;
import org.geowebcache.config.BlobStoreInfo;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.locks.LockProvider;
import org.geowebcache.storage.BlobStore;
import org.geowebcache.storage.StorageException;

import java.util.Arrays;

/**
 * @author shimingen
 * @date 2021/9/8
 */
public class ElasticsearchBlobStoreInfo extends BlobStoreInfo {
    private String hosts;
    private String username;
    private String password;
    private int connectionRequestTimeout;
    private int connectTimeout;
    private int socketTimeout;

    /**
     * x filed name
     */
    private String xTile;
    /** y filed name */
    private String yTile;
    /** z filed name */
    private String zoom;
    /** img filed name ; */
    private String img;

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

    public HttpHost[] getHttpHosts() {
        final String[] split = hosts.split(";");
        return Arrays.stream(split).map(HttpHost::create).toArray(HttpHost[]::new);
    }

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getConnectionRequestTimeout() {
        return connectionRequestTimeout;
    }

    public void setConnectionRequestTimeout(int connectionRequestTimeout) {
        this.connectionRequestTimeout = connectionRequestTimeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    @Override
    public String toString() {
        return "Elasticsearch BlobStore";
    }

    @Override
    public BlobStore createInstance(TileLayerDispatcher layers, LockProvider lockProvider)
            throws StorageException {
        return new ElasticsearchBlobStore(this);
    }

    @Override
    public String getLocation() {
        return "ElasticsearchInfo BlobStore";
    }
}
