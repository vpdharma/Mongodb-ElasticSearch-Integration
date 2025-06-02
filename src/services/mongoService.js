const { MongoClient } = require('mongodb');
const logger = require('../utils/logger');

let db = null;
let client = null;

const connectMongoDB = async () => {
    try {
        const uri = process.env.MONGODB_URI || 'mongodb://localhost:27017';

        client = new MongoClient(uri, {
            useUnifiedTopology: true,
            maxPoolSize: 10
        });

        await client.connect();
        db = client.db(process.env.MONGODB_DATABASE || 'lucene');

        logger.info('Successfully connected to MongoDB');
        return db;
    } catch (error) {
        logger.error('MongoDB connection error:', error);
        throw error;
    }
};

const getDB = () => {
    if (!db) {
        throw new Error('MongoDB not connected. Call connectMongoDB() first.');
    }
    return db;
};

const getClient = () => {
    if (!client) {
        throw new Error('MongoDB client not connected. Call connectMongoDB() first.');
    }
    return client;
};

const getCollection = (collectionName) => {
    const database = getDB();
    const collection = collectionName || process.env.MONGODB_COLLECTION || 'subnet_details';
    return database.collection(collection);
};

const closeMongoDB = async () => {
    try {
        if (client) {
            await client.close();
            db = null;
            client = null;
            logger.info('MongoDB connection closed');
        }
    } catch (error) {
        logger.error('Error closing MongoDB connection:', error);
        throw error;
    }
};

module.exports = {
    connectMongoDB,
    getDB,
    getClient,
    getCollection,
    closeMongoDB
};
