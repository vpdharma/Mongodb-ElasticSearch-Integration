const express = require('express');
const { bulkSyncToElasticsearch } = require('../services/syncService');
const { getCollection } = require('../services/mongoService');
const { getClient } = require('../services/elasticsearchService');
const logger = require('../utils/logger');

const router = express.Router();

/**
 * @route   POST /api/sync/bulk
 * @desc    Manually trigger bulk sync from MongoDB to Elasticsearch
 */
router.post('/bulk', async (req, res) => {
    try {
        logger.info('Manual bulk sync initiated');

        // Get MongoDB collection
        const collection = getCollection();
        const documents = await collection.find({}).toArray();

        if (documents.length === 0) {
            return res.json({
                success: true,
                message: 'No documents found in MongoDB to sync',
                documentsProcessed: 0
            });
        }

        // Get Elasticsearch client
        const esClient = getClient();
        const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

        // FIX: Prepare bulk operation without _id in document body
        const body = documents.flatMap(doc => {
            // Destructure to separate _id from the rest of the document
            const { _id, ...docWithoutId } = doc;

            return [
                { index: { _index: indexName, _id: _id.toString() } },
                docWithoutId  // Document body without _id field
            ];
        });

        // Execute bulk operation
        const response = await esClient.bulk({ refresh: true, body });

        if (response.errors) {
            logger.error('Bulk sync had errors:', response.items);
            return res.status(500).json({
                success: false,
                error: 'Bulk sync completed with errors',
                documentsProcessed: documents.length,
                errors: response.items.filter(item => item.index && item.index.error)
            });
        }

        logger.info(`Successfully synced ${documents.length} documents`);

        res.json({
            success: true,
            message: 'Bulk sync completed successfully',
            documentsProcessed: documents.length,
            timestamp: new Date().toISOString()
        });

    } catch (error) {
        logger.error('Manual bulk sync failed:', error);
        res.status(500).json({
            success: false,
            error: 'Bulk sync failed',
            message: error.message
        });
    }
});

/**
 * @route   GET /api/sync/status
 * @desc    Get synchronization status and statistics
 */
router.get('/status', async (req, res) => {
    try {
        const collection = getCollection();
        const esClient = getClient();

        // Get MongoDB document count
        const mongoCount = await collection.countDocuments();

        // Get Elasticsearch document count
        const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

        let esCount = 0;
        try {
            const esResponse = await esClient.count({ index: indexName });
            esCount = esResponse.body?.count || esResponse.count || 0;
        } catch (esError) {
            if (esError.meta?.statusCode === 404) {
                logger.info('Elasticsearch index does not exist yet');
                esCount = 0;
            } else {
                throw esError;
            }
        }

        res.json({
            mongodb: {
                count: mongoCount,
                database: process.env.MONGODB_DATABASE || 'lucene',
                collection: process.env.MONGODB_COLLECTION || 'subnet_details'
            },
            elasticsearch: {
                count: esCount,
                index: indexName
            },
            synced: mongoCount === esCount,
            difference: mongoCount - esCount,
            timestamp: new Date().toISOString()
        });

    } catch (error) {
        logger.error('Error getting sync status:', error);
        res.status(500).json({
            error: 'Failed to get sync status',
            message: error.message
        });
    }
});

module.exports = router;
