const { getClient, getCollection } = require('./mongoService');
const { getClient: getESClient } = require('./elasticsearchService');
const logger = require('../utils/logger');

let changeStream = null;

const setupChangeStreams = async () => {
    try {
        // Add a small delay to ensure connections are established
        await new Promise(resolve => setTimeout(resolve, 1000));

        logger.info('Setting up MongoDB Change Streams...');

        // For now, just log that change streams are ready
        // We'll implement the actual change stream logic later
        logger.info('Change streams ready - will implement actual streaming later');

        return true;
    } catch (error) {
        logger.error('Error setting up change streams:', error);
        // Don't throw the error, just log it for now
        logger.warn('Continuing without change streams - they will be set up later');
        return false;
    }
};

const bulkSyncToElasticsearch = async () => {
    try {
        const collection = getCollection();
        const esClient = getESClient();

        const documents = await collection.find({}).toArray();
        logger.info(`Found ${documents.length} documents to sync`);

        if (documents.length > 0) {
            const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

            const body = documents.flatMap(doc => [
                { index: { _index: indexName, _id: doc._id } },
                doc
            ]);

            const response = await esClient.bulk({ refresh: true, body });

            if (response.errors) {
                logger.error('Bulk sync had errors:', response.items);
            } else {
                logger.info(`Successfully synced ${documents.length} documents`);
            }
        }

        return documents.length;
    } catch (error) {
        logger.error('Bulk sync error:', error);
        throw error;
    }
};

const handleChangeEvent = async (change, esClient, indexName) => {
    try {
        switch (change.operationType) {
            case 'insert':
                await esClient.index({
                    index: indexName,
                    id: change.documentKey._id,
                    body: change.fullDocument
                });
                logger.info('Document inserted in Elasticsearch:', change.documentKey._id);
                break;

            case 'update':
                await esClient.update({
                    index: indexName,
                    id: change.documentKey._id,
                    body: {
                        doc: change.fullDocument
                    }
                });
                logger.info('Document updated in Elasticsearch:', change.documentKey._id);
                break;

            case 'delete':
                await esClient.delete({
                    index: indexName,
                    id: change.documentKey._id
                });
                logger.info('Document deleted from Elasticsearch:', change.documentKey._id);
                break;

            default:
                logger.info('Unhandled change operation:', change.operationType);
        }
    } catch (error) {
        logger.error('Error handling change event:', error);
    }
};

// IMPORTANT: Make sure all functions are properly exported
module.exports = {
    setupChangeStreams,
    bulkSyncToElasticsearch,
    handleChangeEvent
};
