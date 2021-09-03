package org.geowebcache.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

import java.util.Objects;

/**
 * @author shimingen
 * @date 2021/8/25
 */
public class MongoManager implements AutoCloseable {
    private static Log LOGGER = LogFactory.getLog(MongoManager.class);
    private final MongoClient mongoClient;
    private final MongoClientURI mongoClientURI;
    private final MongoDatabase database;
    public static final String METADATA = "metadata";
    private final String x;
    private final String y;
    private final String z;
    private final String img;

    public MongoManager(MongoBlobStoreInfo info) {
        mongoClientURI = new MongoClientURI(info.getMongoUrl());
        mongoClient = new MongoClient(mongoClientURI);
        database = mongoClient.getDatabase(mongoClientURI.getDatabase());
        this.x = info.getxTile();
        this.y = info.getyTile();
        this.z = info.getZoom();
        this.img = info.getImg();
    }

    public boolean deleteCollection(String layerName) {
        if (!collectionExist(layerName)) {
            return false;
        }
        final MongoIterable<String> collectionNames = database.listCollectionNames();
        for (String name : collectionNames) {
            if (name.startsWith(layerName)) {
                final MongoCollection<Document> collection = database.getCollection(layerName);
                collection.drop();
            }
        }
        return true;
    }

    public Document getTile(String layerName, long xyz[]) {
        if (!collectionExist(layerName)) {
            database.createCollection(layerName);
        }
        final MongoCollection<Document> collection = database.getCollection(layerName);
        final Bson col = Filters.eq(x, xyz[0]);
        final Bson row = Filters.eq(y, xyz[1]);
        final Bson level = Filters.eq(z, xyz[2]);
        final Bson and = Filters.and(level, row, col);
        return getDocument(layerName, and);
    }

    private Document getDocument(String layerName, Bson filter) {
        final MongoCollection<Document> collection = database.getCollection(layerName);
        return collection.find(filter).first();
    }

    public boolean putTile(String layerName, Document document) {
        if (!collectionExist(layerName)) {
            database.createCollection(layerName);
        }
        final Bson col = Filters.eq(x, document.get(x));
        final Bson row = Filters.eq(y, document.get(y));
        final Bson level = Filters.eq(z, document.get(z));
        final Bson and = Filters.and(level, row, col);
        final Document document1 = getDocument(layerName, and);
        if (Objects.nonNull(document1)) {
            updateDocument(layerName, document);
            return true;
        } else {
            saveDocument(layerName, document);
            return false;
        }
    }

    private void updateDocument(String collection, Document document) {
        final Bson col = Filters.eq(x, document.get(x));
        final Bson row = Filters.eq(y, document.get(y));
        final Bson level = Filters.eq(z, document.get(z));
        final Bson and = Filters.and(level, row, col);
        final MongoCollection<Document> collection0 = database.getCollection(collection);
        Document imgDoc = new Document();
        imgDoc.put(img, document.get(img));
        collection0.updateOne(and, imgDoc);
    }

    private void saveDocument(String collection, Document document) {
        final MongoCollection<Document> col = database.getCollection(collection);
        col.insertOne(document);
    }

    public boolean renameCollection(String newLayerName, String oldLayerName) {
        if (!collectionExist(oldLayerName)) {
            throw new NoSuchCollectionException();
        }
        if (collectionExist(newLayerName)) {
            throw new InvalidCollectionException("new layer name is exist");
        }
        database.getCollection(oldLayerName)
                .renameCollection(new MongoNamespace(database.getName(), newLayerName));
        return true;
    }

    public boolean deleteTile(String layerName, long xyz[]) {
        if (!collectionExist(layerName)) {
            throw new NoSuchCollectionException();
        }
        final MongoCollection<Document> collection = database.getCollection(layerName);
        final Bson col = Filters.eq(y, xyz[1]);
        final Bson row = Filters.eq(x, xyz[0]);
        final Bson level = Filters.eq(z, xyz[2]);
        final Bson and = Filters.and(level, row, col);
        final DeleteResult deleteResult = collection.deleteOne(and);
        return deleteResult.wasAcknowledged();
    }

    public void createCollection(String collection) {
        if (collectionExist(collection)) {
            return;
        }
        database.createCollection(collection);
    }

    public Binary getBinary(Document tile) {
        return tile.get(img, Binary.class);
    }

    /**
     * 坐标转换
     *
     * @param xyz xyz 坐标
     * @return 坐标
     */
    public long[] trans(long xyz[]) {
        long[] ret = new long[3];
        long y = xyz[1];
        long z = xyz[2];
        if (z > 0) {
            long val = (2L << (z - 1)) - 1;
            xyz[1] = val - y;
        }
        ret[0] = xyz[0];
        ret[1] = xyz[1];
        ret[2] = xyz[2];
        return ret;
    }

    /**
     * 坐标转换
     *
     * @param xyz xyz 坐标
     * @return 坐标
     */
    public long[] readTrans(long xyz[]) {
        long[] ret = new long[3];
        ret[0] = xyz[0];
        long z = xyz[2];
        if (z > 0) {
            long val = (2L << (z - 1)) - 1;
            ret[1] = val - xyz[1];
        } else {
            ret[1] = xyz[1];
        }
        ret[2] = xyz[2];
        return ret;
    }

    public boolean collectionExist(String collection) {
        for (String col : database.listCollectionNames()) {
            if (Objects.equals(collection, col)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        this.mongoClient.close();
    }

    public void putEntry(String layerName, String key, String value) {
        Document document = new Document();
        document.put("_id", new BsonObjectId());
        document.put("key", key);
        document.put("value", value);
        document.put("layer", layerName);
        saveDocument(METADATA, document);
    }

    public String getEntry(String layerName, String key) {
        final Bson layer = Filters.eq("layer", layerName);
        final Bson key0 = Filters.eq("key", key);
        final Bson and = Filters.and(key0, layer);
        final Document document = getDocument(METADATA, and);
        return (String) document.getOrDefault("value", "");
    }

    public Document generateDocument(long x, long y, long z, byte[] img) {
        Document document = new Document();
        document.put("_id", String.format("%d_%d_%d", x, y, z));
        document.put(this.z, z);
        document.put(this.y, y);
        document.put(this.x, x);
        document.put(this.img, new Binary(img));
        return document;
    }
}
