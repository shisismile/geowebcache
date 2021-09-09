package org.geowebcache.elasticsearch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.io.ByteArrayResource;
import org.geowebcache.io.Resource;
import org.geowebcache.storage.*;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * @author shimingen
 * @date 2021/9/8
 */
public class ElasticsearchBlobStore implements BlobStore {
    private static Log LOGGER = LogFactory.getLog(ElasticsearchBlobStore.class);
    private final ElasticsearchManager elasticsearchManager;
    private final BlobStoreListenerList listeners = new BlobStoreListenerList();

    public ElasticsearchBlobStore(ElasticsearchBlobStoreInfo info) {
        elasticsearchManager = new ElasticsearchManager(info);
    }

    private String getReplaceName(String name) {
        return name.toLowerCase().replace(":", "_");
    }

    @Override
    public boolean delete(String layerName) throws StorageException {
        // layerName world_map:OK_WSG84
        boolean ret = elasticsearchManager.deleteIndex(getReplaceName(layerName));
        this.listeners.sendLayerDeleted(layerName);
        return ret;
    }

    @Override
    public boolean deleteByGridsetId(String layerName, String gridSetId) throws StorageException {
        final String indexName = getReplaceName(layerName);
        listeners.sendGridSubsetDeleted(layerName, gridSetId);
        return elasticsearchManager.deleteIndex(indexName);
    }

    @Override
    public boolean deleteByParametersId(String layerName, String parametersId)
            throws StorageException {
        return false;
    }

    @Override
    public boolean delete(TileObject obj) throws StorageException {
        final long[] trans = elasticsearchManager.trans(obj.getXYZ());
        boolean ret = elasticsearchManager.deleteTile(getReplaceName(obj.getLayerName()), trans);
        if (ret) {
            listeners.sendTileDeleted(obj);
            obj.setBlobSize(0);
        }
        return ret;
    }

    @Override
    public boolean delete(TileRange obj) throws StorageException {
        return false;
    }

    @Override
    public boolean get(TileObject obj) throws StorageException {
        LOGGER.info("TileObject Read: " + obj);
        final long[] xyz = obj.getXYZ();
        final long[] trans = elasticsearchManager.readTrans(xyz);
        LOGGER.info("trans : " + Arrays.toString(trans));
        final byte[] img = elasticsearchManager.getImg(getReplaceName(obj.getLayerName()), trans);
        if (Objects.isNull(img) || img.length == 0) {
            return false;
        } else {
            ByteArrayResource resource = new ByteArrayResource(img);
            obj.setBlob(resource);
            obj.setCreated(resource.getLastModified());
            obj.setBlobSize((int) resource.getSize());
            return true;
        }
    }

    @Override
    public void put(TileObject obj) throws StorageException {
        LOGGER.info("TileObject Write: " + obj);
        final long[] xyz = obj.getXYZ();
        final long[] trans = elasticsearchManager.trans(xyz);
        byte[] img;
        try {
            final Resource blob = obj.getBlob();
            final ByteArrayOutputStream buf = new ByteArrayOutputStream(4096);
            final InputStream inputStream = blob.getInputStream();
            byte[] buff = new byte[4096];
            int rc = 0;
            while ((rc = inputStream.read(buff, 0, 4096)) > 0) {
                buf.write(buff, 0, rc);
            }
            img = buf.toByteArray();
        } catch (Exception e) {
            throw new StorageException("put failed", e);
        }
        final boolean b =
                elasticsearchManager.putTile(getReplaceName(obj.getLayerName()), trans, img);
        if (b) {
            listeners.sendTileUpdated(obj, 0);
        } else {
            listeners.sendTileStored(obj);
        }
    }

    @Override
    public void clear() throws StorageException {
    }

    @Override
    public void destroy() {
    }

    @Override
    public void addListener(BlobStoreListener listener) {
        listeners.addListener(listener);
    }

    @Override
    public boolean removeListener(BlobStoreListener listener) {
        return listeners.removeListener(listener);
    }

    @Override
    public boolean rename(String oldLayerName, String newLayerName) throws StorageException {
        if (!elasticsearchManager.checkIndex(oldLayerName)) {
            this.listeners.sendLayerRenamed(oldLayerName, newLayerName);
            return true;
        }
        this.listeners.sendLayerRenamed(oldLayerName, newLayerName);
        boolean ret = elasticsearchManager.renameIndex(newLayerName, oldLayerName);
        if (ret) {
            this.listeners.sendLayerRenamed(oldLayerName, newLayerName);
        }
        return ret;
    }

    @Override
    public String getLayerMetadata(String layerName, String key) {
        return elasticsearchManager.getMeta(getReplaceName(layerName), key);
    }

    @Override
    public void putLayerMetadata(String layerName, String key, String value) {
        elasticsearchManager.putMeta(getReplaceName(layerName), key, value);
    }

    @Override
    public boolean layerExists(String layerName) {
        return elasticsearchManager.checkIndex(getReplaceName(layerName));
    }

    @Override
    public Map<String, Optional<Map<String, String>>> getParametersMapping(String layerName) {
        return null;
    }
}
