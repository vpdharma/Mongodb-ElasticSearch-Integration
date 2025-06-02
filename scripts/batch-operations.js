const { MongoClient } = require('mongodb');
const dotenv = require('dotenv');
const { Client } = require('@elastic/elasticsearch');

dotenv.config();

async function performBatchOperations() {
    // Connect to MongoDB
    const mongoClient = new MongoClient(process.env.MONGODB_URI);
    await mongoClient.connect();
    console.log('Connected to MongoDB');

    // Connect to Elasticsearch
    const esClient = new Client({
        node: process.env.ELASTICSEARCH_NODE,
        auth: {
            username: process.env.ELASTICSEARCH_USERNAME,
            password: process.env.ELASTICSEARCH_PASSWORD
        }
    });
    console.log('Connected to Elasticsearch');

    try {
        const db = mongoClient.db(process.env.MONGODB_DATABASE);
        const collection = db.collection(process.env.MONGODB_COLLECTION);
        const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

        // Example 1: Bulk insert into MongoDB
        console.log('\n--- BULK INSERT INTO MONGODB ---');
        const bulkData = generateBulkData(20);
        const insertResult = await collection.insertMany(bulkData);
        console.log(`Inserted ${insertResult.insertedCount} documents into MongoDB`);

        // Example 2: Bulk index into Elasticsearch
        console.log('\n--- BULK INDEX INTO ELASTICSEARCH ---');
        const allDocs = await collection.find({}).toArray();

        const operations = allDocs.flatMap(doc => [
            { index: { _index: indexName, _id: doc._id.toString() } },
            transformForElasticsearch(doc)
        ]);

        const bulkResponse = await esClient.bulk({ body: operations });

        if (bulkResponse.errors) {
            console.log('Bulk indexing encountered errors');
        } else {
            console.log(`Bulk indexed ${allDocs.length} documents into Elasticsearch`);
        }

        // Example 3: Verify synchronization
        console.log('\n--- VERIFY SYNCHRONIZATION ---');
        const mongoCount = await collection.countDocuments();
        const esCountResponse = await esClient.count({ index: indexName });
        const esCount = esCountResponse.body.count;

        console.log(`MongoDB document count: ${mongoCount}`);
        console.log(`Elasticsearch document count: ${esCount}`);
        console.log(`Synchronized: ${mongoCount === esCount ? 'Yes' : 'No'}`);

    } catch (error) {
        console.error('Error during batch operations:', error);
    } finally {
        await mongoClient.close();
        console.log('MongoDB connection closed');
    }
}

// Helper function to generate test data
function generateBulkData(count) {
    const data = [];

    for (let i = 0; i < count; i++) {
        data.push({
            CLUSTERID: `cluster-${100 + i}`,
            CIDR: `192.168.${i}.0/24`,
            CIDRIPV4: `192.168.${i}.0/24`,
            IPV4: `192.168.${i}.1`,
            IP: `192.168.${i}.1`,
            SITE: `site-${i % 5 + 1}`,
            DESCRIPTION: `Test subnet ${i} for ${i % 2 === 0 ? 'development' : 'production'}`,
            TIMESTAMP: new Date(),
            USERNAME: `user${i % 3 + 1}`
        });
    }

    return data;
}

// Transform MongoDB document for Elasticsearch
function transformForElasticsearch(doc) {
    const suggest = [];

    if (doc.CLUSTERID) suggest.push(doc.CLUSTERID);
    if (doc.SITE) suggest.push(doc.SITE);
    if (doc.DESCRIPTION) suggest.push(doc.DESCRIPTION);

    return {
        ...doc,
        suggest: {
            input: suggest,
            contexts: {
                category: ['subnet', 'network']
            }
        }
    };
}

// Run the batch operations
performBatchOperations().catch(console.error);
