const express = require('express');
const searchController = require('../controllers/searchController');
const { validateSearch, validateAutocomplete } = require('../middleware/validation');

const router = express.Router();

/**
 * @route   GET /api/search
 * @desc    Perform full-text search with optional fuzzy matching
 * @query   q (required) - search query
 * @query   field (optional) - specific field to search
 * @query   fuzzy (optional) - enable fuzzy search (true/false)
 * @query   fuzziness (optional) - fuzziness level (AUTO, 0, 1, 2)
 * @query   size (optional) - number of results (default: 10)
 * @query   from (optional) - offset for pagination (default: 0)
 * @query   sortBy (optional) - field to sort by (default: _score)
 * @query   sortOrder (optional) - sort order (asc/desc, default: desc)
 */
router.get('/', validateSearch, searchController.search);

/**
 * @route   GET /api/search/autocomplete
 * @desc    Get autocomplete suggestions
 * @query   q (required) - search query prefix
 * @query   size (optional) - number of suggestions (default: 5)
 */
router.get('/autocomplete', validateAutocomplete, searchController.autocomplete);

/**
 * @route   GET /api/search/advanced
 * @desc    Perform advanced search with filters
 * @query   q (optional) - search query
 * @query   clusters (optional) - filter by cluster IDs
 * @query   sites (optional) - filter by sites
 * @query   usernames (optional) - filter by usernames
 * @query   dateFrom (optional) - start date filter
 * @query   dateTo (optional) - end date filter
 * @query   size (optional) - number of results (default: 10)
 * @query   from (optional) - offset for pagination (default: 0)
 */
router.get('/advanced', searchController.advancedSearch);

/**
 * @route   GET /api/search/document/:id
 * @desc    Get document by ID
 * @param   id - document ID
 */
router.get('/document/:id', searchController.getById);

// Add these routes to your searchRoutes.js

/**
 * @route   GET /api/search/debug/count
 * @desc    Get total document count in Elasticsearch
 */
router.get('/debug/count', async (req, res) => {
    try {
        const esClient = require('../services/elasticsearchService').getClient();
        const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

        const response = await esClient.count({ index: indexName });

        res.json({
            index: indexName,
            count: response.body?.count || response.count || 0,
            raw_response: response.body || response
        });
    } catch (error) {
        res.status(500).json({
            error: error.message,
            details: error.meta || error
        });
    }
});

/**
 * @route   GET /api/search/debug/raw
 * @desc    Get raw documents from Elasticsearch
 */
router.get('/debug/raw', async (req, res) => {
    try {
        const esClient = require('../services/elasticsearchService').getClient();
        const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

        const response = await esClient.search({
            index: indexName,
            body: {
                query: { match_all: {} },
                size: 3
            }
        });

        res.json({
            index: indexName,
            total: response.body?.hits?.total || response.hits?.total || 0,
            documents: response.body?.hits?.hits || response.hits?.hits || [],
            raw_response: response.body || response
        });
    } catch (error) {
        res.status(500).json({
            error: error.message,
            details: error.meta || error
        });
    }
});

/**
 * @route   GET /api/search/debug/mapping
 * @desc    Get index mapping
 */
router.get('/debug/mapping', async (req, res) => {
    try {
        const esClient = require('../services/elasticsearchService').getClient();
        const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

        const response = await esClient.indices.getMapping({ index: indexName });

        res.json({
            index: indexName,
            mapping: response.body || response
        });
    } catch (error) {
        res.status(500).json({
            error: error.message,
            details: error.meta || error
        });
    }
});

// Add these routes to your searchRoutes.js

/**
 * @route   GET /api/search/debug/count
 * @desc    Get total document count in Elasticsearch
 */
router.get('/debug/count', async (req, res) => {
    try {
        const esClient = require('../services/elasticsearchService').getClient();
        const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

        const response = await esClient.count({ index: indexName });

        res.json({
            index: indexName,
            count: response.body?.count || response.count || 0,
            raw_response: response.body || response
        });
    } catch (error) {
        res.status(500).json({
            error: error.message,
            details: error.meta || error
        });
    }
});

/**
 * @route   GET /api/search/debug/raw
 * @desc    Get raw documents from Elasticsearch
 */
router.get('/debug/raw', async (req, res) => {
    try {
        const esClient = require('../services/elasticsearchService').getClient();
        const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

        const response = await esClient.search({
            index: indexName,
            body: {
                query: { match_all: {} },
                size: 3
            }
        });

        res.json({
            index: indexName,
            total: response.body?.hits?.total || response.hits?.total || 0,
            documents: response.body?.hits?.hits || response.hits?.hits || [],
            raw_response: response.body || response
        });
    } catch (error) {
        res.status(500).json({
            error: error.message,
            details: error.meta || error
        });
    }
});

/**
 * @route   GET /api/search/debug/mapping
 * @desc    Get index mapping
 */
router.get('/debug/mapping', async (req, res) => {
    try {
        const esClient = require('../services/elasticsearchService').getClient();
        const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

        const response = await esClient.indices.getMapping({ index: indexName });

        res.json({
            index: indexName,
            mapping: response.body || response
        });
    } catch (error) {
        res.status(500).json({
            error: error.message,
            details: error.meta || error
        });
    }
});

module.exports = router;
