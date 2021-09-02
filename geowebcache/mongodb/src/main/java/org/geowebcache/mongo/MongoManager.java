package org.geowebcache.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;

import java.util.LinkedList;
import java.util.List;

/**
 * @author shimingen
 * @date 2021/8/25
 */
public class MongoManager implements AutoCloseable {
    private static Log LOGGER = LogFactory.getLog(MongoManager.class);
    private final MongoClient mongoClient;
    private final MongoClientURI mongoClientURI;
    private final MongoDatabase database;

    public MongoManager(String mongoUrl) {
        mongoClientURI = new MongoClientURI(mongoUrl);
        mongoClient = new MongoClient(mongoUrl);
        database = mongoClient.getDatabase(mongoClientURI.getDatabase());
    }

    public void deleteCollection(String layerName) {
        final MongoCollection<Document> collection = database.getCollection(layerName);
        collection.drop();
    }

    public List<Document> getDocuments(String layerName) {
        final MongoCollection<Document> collection = database.getCollection(layerName);
        final FindIterable<Document> documents = collection.find();
        List<Document> documentList = new LinkedList<>();
        for (Document document : documents) {
            documentList.add(document);
        }
        return documentList;
    }


    @Override
    public void close() throws Exception {
        this.mongoClient.close();
    }
}
