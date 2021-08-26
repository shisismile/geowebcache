package org.geowebcache.mongo;

import org.geowebcache.storage.*;

import java.util.Map;
import java.util.Optional;

/**
 * @author shimingen
 * @date 2021/8/26
 */
public class MongoBlobStore implements BlobStore {
    @Override
    public boolean delete(String layerName) throws StorageException {
        return false;
    }

    @Override
    public boolean deleteByGridsetId(String layerName, String gridSetId) throws StorageException {
        return false;
    }

    @Override
    public boolean deleteByParametersId(String layerName, String parametersId) throws StorageException {
        return false;
    }

    @Override
    public boolean delete(TileObject obj) throws StorageException {
        return false;
    }

    @Override
    public boolean delete(TileRange obj) throws StorageException {
        return false;
    }

    @Override
    public boolean get(TileObject obj) throws StorageException {
        return false;
    }

    @Override
    public void put(TileObject obj) throws StorageException {

    }

    @Override
    public void clear() throws StorageException {

    }

    @Override
    public void destroy() {

    }

    @Override
    public void addListener(BlobStoreListener listener) {

    }

    @Override
    public boolean removeListener(BlobStoreListener listener) {
        return false;
    }

    @Override
    public boolean rename(String oldLayerName, String newLayerName) throws StorageException {
        return false;
    }

    @Override
    public String getLayerMetadata(String layerName, String key) {
        return null;
    }

    @Override
    public void putLayerMetadata(String layerName, String key, String value) {

    }

    @Override
    public boolean layerExists(String layerName) {
        return false;
    }

    @Override
    public Map<String, Optional<Map<String, String>>> getParametersMapping(String layerName) {
        return null;
    }
}
