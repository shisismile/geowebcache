package org.geowebcache.mongo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.bson.types.Binary;
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
 * @date 2021/8/26
 */
public class MongoBlobStore implements BlobStore {
    private static Log LOGGER = LogFactory.getLog(MongoBlobStore.class);

    private final MongoManager mongoManager;
    private final BlobStoreListenerList listeners = new BlobStoreListenerList();

    public MongoBlobStore(MongoBlobStoreInfo configuration) {
        this.mongoManager = new MongoManager(configuration);
        this.mongoManager.createCollection(MongoManager.METADATA);
    }

    @Override
    public boolean delete(String layerName) throws StorageException {
        // layerName world_map:OK_WSG84
        boolean ret = mongoManager.deleteCollection(getReplaceName(layerName));
        this.listeners.sendLayerDeleted(layerName);
        return ret;
    }

    @Override
    public boolean deleteByGridsetId(String layerName, String gridSetId) throws StorageException {
        final String collectionName = getCollectionName(layerName, gridSetId);
        listeners.sendGridSubsetDeleted(layerName, gridSetId);
        return mongoManager.deleteCollection(collectionName);
    }

    @Override
    public boolean deleteByParametersId(String layerName, String parametersId)
            throws StorageException {
        listeners.sendParametersDeleted(layerName, parametersId);
        return false;
    }

    @Override
    public boolean delete(TileObject obj) throws StorageException {
        final long[] trans = mongoManager.trans(obj.getXYZ());
        boolean ret =
                mongoManager.deleteTile(
                        getCollectionName(obj.getLayerName(), obj.getGridSetId()), trans);
        if (ret) {
            listeners.sendTileDeleted(obj);
            obj.setBlobSize(0);
        }
        return ret;
    }

    @Override
    public boolean delete(TileRange obj) throws StorageException {
        final String layerName = obj.getLayerName();
        //        listeners.sendTileDeleted(
        //                layerName,
        //                gridSetId,
        //                blobFormat,
        //                parametersId,
        //                x,
        //                y,
        //                z,
        //                padSize(length));
        return false;
    }

    @Override
    public boolean get(TileObject obj) throws StorageException {
        // TileObject Read: [world_map:OK_WSG84,EPSG:900913,{[2, 1, 2]}]
        LOGGER.info("TileObject Read: " + obj);
        final long[] xyz = obj.getXYZ();
        final long[] trans = mongoManager.readTrans(xyz);
        LOGGER.info("trans : " + Arrays.toString(trans));
        final Document tile =
                mongoManager.getTile(
                        getCollectionName(obj.getLayerName(), obj.getGridSetId()), trans);
        if (Objects.isNull(tile)) {
            return false;
        } else {
            final Binary img = mongoManager.getBinary(tile);
            ByteArrayResource resource = new ByteArrayResource(img.getData());
            obj.setBlob(resource);
            obj.setCreated(resource.getLastModified());
            obj.setBlobSize((int) resource.getSize());
            return true;
        }
    }

    private String getCollectionName(String layerName, String gridSetId) {
        return getReplaceName(layerName) + "_" + getReplaceName(gridSetId);
    }

    private String getReplaceName(String name) {
        return name.replace(":", "_");
    }

    @Override
    public void put(TileObject obj) throws StorageException {
        LOGGER.info("TileObject Write: " + obj);
        final long[] xyz = obj.getXYZ();
        final long[] trans = mongoManager.trans(xyz);
        long x = trans[0];
        long y = trans[1];
        long z = trans[2];
        Document document;
        try {
            final Resource blob = obj.getBlob();
            final ByteArrayOutputStream buf = new ByteArrayOutputStream(4096);
            final InputStream inputStream = blob.getInputStream();
            byte[] buff = new byte[4096];
            int rc = 0;
            while ((rc = inputStream.read(buff, 0, 4096)) > 0) {
                buf.write(buff, 0, rc);
            }
            document = mongoManager.generateDocument(x, y, z, buf.toByteArray());
        } catch (Exception e) {
            throw new StorageException("put failed", e);
        }
        final boolean b =
                mongoManager.putTile(
                        getCollectionName(obj.getLayerName(), obj.getGridSetId()), document);
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
        if (!mongoManager.collectionExist(oldLayerName)) {
            this.listeners.sendLayerRenamed(oldLayerName, newLayerName);
            return true;
        }
        this.listeners.sendLayerRenamed(oldLayerName, newLayerName);
        boolean ret = mongoManager.renameCollection(newLayerName, oldLayerName);
        if (ret) {
            this.listeners.sendLayerRenamed(oldLayerName, newLayerName);
        }
        return ret;
    }

    @Override
    public String getLayerMetadata(String layerName, String key) {
        return mongoManager.getEntry(layerName, key);
    }

    @Override
    public void putLayerMetadata(String layerName, String key, String value) {
        mongoManager.putEntry(layerName, key, value);
    }

    @Override
    public boolean layerExists(String layerName) {
        return mongoManager.collectionExist(layerName);
    }

    @Override
    public Map<String, Optional<Map<String, String>>> getParametersMapping(String layerName) {
        return null;
    }
}
