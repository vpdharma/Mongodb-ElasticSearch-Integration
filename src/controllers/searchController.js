const { getClient } = require('../services/elasticsearchService');
const logger = require('../utils/logger');

// Helper functions defined outside the object to avoid context issues
const buildMultiFieldQuery = async (q, fuzzy, fuzziness) => {
    const baseQuery = {
        bool: {
            should: [
                // Exact matches (highest priority)
                {
                    bool: {
                        should: [
                            { term: { 'CLUSTERID': { value: q, boost: 3.0 } } },
                            { term: { 'SITE': { value: q, boost: 3.0 } } },
                            { term: { 'USERNAME': { value: q, boost: 2.0 } } }
                        ]
                    }
                },
                // Wildcard searches for partial matches
                {
                    bool: {
                        should: [
                            { wildcard: { 'CLUSTERID': { value: `*${q.toLowerCase()}*`, boost: 2.5 } } },
                            { wildcard: { 'SITE': { value: `*${q.toLowerCase()}*`, boost: 2.5 } } },
                            { wildcard: { 'USERNAME': { value: `*${q.toLowerCase()}*`, boost: 1.5 } } },
                            { wildcard: { 'VALUE': { value: `*${q.toLowerCase()}*`, boost: 2.0 } } }
                        ]
                    }
                },
                // Text search in VALUE field
                {
                    match: {
                        'VALUE': {
                            query: q,
                            fuzziness: fuzzy ? fuzziness : 0,
                            boost: 2.0
                        }
                    }
                },
                // Autocomplete fields
                {
                    bool: {
                        should: [
                            { match: { 'CLUSTERID.autocomplete': { query: q, boost: 2.0 } } },
                            { match: { 'SITE.autocomplete': { query: q, boost: 2.0 } } }
                        ]
                    }
                },
                // Prefix matches
                {
                    bool: {
                        should: [
                            { prefix: { 'CLUSTERID': { value: q.toLowerCase(), boost: 1.5 } } },
                            { prefix: { 'SITE': { value: q.toLowerCase(), boost: 1.5 } } },
                            { prefix: { 'USERNAME': { value: q.toLowerCase(), boost: 1.0 } } }
                        ]
                    }
                }
            ],
            minimum_should_match: 1
        }
    };

    return baseQuery;
};

const buildFieldSpecificQuery = async (q, field, fuzzy, fuzziness) => {
    const fieldMapping = {
        'CLUSTERID': 'keyword',
        'SITE': 'keyword',
        'VALUE': 'text',
        'USERNAME': 'keyword',
        'CIDR': 'keyword',
        'CIDRIPV4': 'keyword'
    };

    const fieldType = fieldMapping[field.toUpperCase()];

    if (!fieldType) {
        throw new Error(`Invalid field: ${field}`);
    }

    if (fieldType === 'keyword') {
        return {
            bool: {
                should: [
                    { term: { [field]: { value: q, boost: 3.0 } } },
                    { wildcard: { [field]: { value: `*${q.toLowerCase()}*`, boost: 2.0 } } },
                    { prefix: { [field]: { value: q.toLowerCase(), boost: 1.5 } } }
                ],
                minimum_should_match: 1
            }
        };
    } else {
        return {
            match: {
                [field]: {
                    query: q,
                    fuzziness: fuzzy ? fuzziness : 0
                }
            }
        };
    }
};

const buildSortConfig = (sortBy, sortOrder) => {
    const validSortFields = ['_score', 'TIMESTAMP', 'CLUSTERID', 'SITE', 'USERNAME'];
    const field = validSortFields.includes(sortBy) ? sortBy : '_score';
    const order = ['asc', 'desc'].includes(sortOrder) ? sortOrder : 'desc';

    if (field === '_score') {
        return [{ '_score': { order: order } }];
    }

    return [
        { [field]: { order: order } },
        { '_score': { order: 'desc' } }
    ];
};

const formatAutocompleteSuggestions = (hits, query, field) => {
    const suggestions = new Set();

    hits.forEach(hit => {
        const source = hit._source;

        if (field) {
            if (source[field] && source[field].toLowerCase().includes(query.toLowerCase())) {
                suggestions.add(source[field]);
            }
        } else {
            // Add relevant fields that match the query
            ['CLUSTERID', 'SITE', 'USERNAME'].forEach(fieldName => {
                if (source[fieldName] && source[fieldName].toLowerCase().includes(query.toLowerCase())) {
                    suggestions.add(source[fieldName]);
                }
            });

            // Add VALUE field matches
            if (source.VALUE && source.VALUE.toLowerCase().includes(query.toLowerCase())) {
                suggestions.add(source.VALUE);
            }
        }
    });

    return Array.from(suggestions).slice(0, 10).map(text => ({ text, type: 'suggestion' }));
};

// Main controller object
const searchController = {
    /**
     * Main search function with support for multiple search types
     */
    async search(req, res) {
        try {
            const {
                q,
                field,
                fuzzy = false,
                fuzziness = 'AUTO',
                size = 10,
                from = 0,
                sortBy = '_score',
                sortOrder = 'desc'
            } = req.query;

            if (!q) {
                return res.status(400).json({ error: 'Query parameter "q" is required' });
            }

            const esClient = getClient();
            const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

            let query;

            // Handle wildcard search (*)
            if (q === '*') {
                query = { match_all: {} };
            } else if (field) {
                // Field-specific search - FIX: Use function reference instead of this
                query = await buildFieldSpecificQuery(q, field, fuzzy, fuzziness);
            } else {
                // Multi-field search - FIX: Use function reference instead of this
                query = await buildMultiFieldQuery(q, fuzzy, fuzziness);
            }

            // Build sort configuration - FIX: Use function reference instead of this
            const sort = buildSortConfig(sortBy, sortOrder);

            const response = await esClient.search({
                index: indexName,
                body: {
                    query: query,
                    size: parseInt(size),
                    from: parseInt(from),
                    sort: sort,
                    highlight: {
                        fields: {
                            'CLUSTERID': { type: 'unified' },
                            'SITE': { type: 'unified' },
                            'VALUE': { type: 'unified' },
                            'USERNAME': { type: 'unified' },
                            'CLUSTERID.autocomplete': { type: 'unified' },
                            'SITE.autocomplete': { type: 'unified' }
                        },
                        pre_tags: ['<mark>'],
                        post_tags: ['</mark>']
                    }
                }
            });

            const hits = response.body?.hits || response.hits || { total: { value: 0 }, hits: [] };
            const totalValue = hits.total?.value || hits.total || 0;

            res.json({
                total: { value: totalValue, relation: hits.total?.relation || "eq" },
                hits: hits.hits?.map(hit => ({
                    id: hit._id,
                    score: hit._score,
                    source: hit._source,
                    highlight: hit.highlight || {}
                })) || []
            });

        } catch (error) {
            logger.error('Search error:', error);

            if (error.meta?.statusCode === 404) {
                return res.status(404).json({
                    error: 'Search index not found',
                    message: 'Please run bulk sync first to create the index'
                });
            }

            res.status(500).json({
                error: 'Search failed',
                message: process.env.NODE_ENV === 'development' ? error.message : 'Internal server error'
            });
        }
    },

    /**
     * Advanced autocomplete with better suggestions
     */
    async autocomplete(req, res) {
        try {
            const { q, size = 5, field } = req.query;

            if (!q || q.length < 1) {
                return res.status(400).json({ error: 'Query parameter "q" is required and must be at least 1 character' });
            }

            const esClient = getClient();
            const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

            let query;

            if (field) {
                // Field-specific autocomplete
                query = {
                    bool: {
                        should: [
                            { prefix: { [field]: q.toLowerCase() } },
                            { wildcard: { [field]: `*${q.toLowerCase()}*` } }
                        ]
                    }
                };
            } else {
                // Multi-field autocomplete
                query = {
                    bool: {
                        should: [
                            // Prefix matches
                            { prefix: { 'CLUSTERID': { value: q.toLowerCase(), boost: 3.0 } } },
                            { prefix: { 'SITE': { value: q.toLowerCase(), boost: 3.0 } } },
                            { prefix: { 'USERNAME': { value: q.toLowerCase(), boost: 2.0 } } },
                            // Autocomplete fields
                            { match: { 'CLUSTERID.autocomplete': { query: q, boost: 2.5 } } },
                            { match: { 'SITE.autocomplete': { query: q, boost: 2.5 } } },
                            // Wildcard matches
                            { wildcard: { 'VALUE': `*${q.toLowerCase()}*` } }
                        ],
                        minimum_should_match: 1
                    }
                };
            }

            const response = await esClient.search({
                index: indexName,
                body: {
                    query: query,
                    size: parseInt(size),
                    _source: ['CLUSTERID', 'SITE', 'VALUE', 'USERNAME']
                }
            });

            const hits = response.body?.hits || response.hits || { hits: [] };
            const suggestions = formatAutocompleteSuggestions(hits.hits, q, field);

            res.json({
                query: q,
                suggestions: suggestions
            });

        } catch (error) {
            logger.error('Autocomplete error:', error);
            res.status(500).json({ error: 'Autocomplete failed', message: error.message });
        }
    },

    /**
     * Advanced search with filters
     */
    async advancedSearch(req, res) {
        try {
            const {
                q,
                sites,
                clusters,
                usernames,
                dateFrom,
                dateTo,
                size = 10,
                from = 0,
                sortBy = '_score',
                sortOrder = 'desc'
            } = req.query;

            const esClient = getClient();
            const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

            let query = { bool: { must: [], filter: [] } };

            // Text search
            if (q) {
                query.bool.must.push(await buildMultiFieldQuery(q, false, 'AUTO'));
            }

            // Filters
            if (sites) {
                query.bool.filter.push({
                    terms: { 'SITE': sites.split(',').map(s => s.trim()) }
                });
            }

            if (clusters) {
                query.bool.filter.push({
                    terms: { 'CLUSTERID': clusters.split(',').map(c => c.trim()) }
                });
            }

            if (usernames) {
                query.bool.filter.push({
                    terms: { 'USERNAME': usernames.split(',').map(u => u.trim()) }
                });
            }

            // Date range filter
            if (dateFrom || dateTo) {
                const dateRange = {};
                if (dateFrom) dateRange.gte = dateFrom;
                if (dateTo) dateRange.lte = dateTo;

                query.bool.filter.push({
                    range: { 'TIMESTAMP': dateRange }
                });
            }

            // If no conditions, use match_all
            if (query.bool.must.length === 0 && query.bool.filter.length === 0) {
                query = { match_all: {} };
            }

            const sort = buildSortConfig(sortBy, sortOrder);

            const response = await esClient.search({
                index: indexName,
                body: {
                    query: query,
                    size: parseInt(size),
                    from: parseInt(from),
                    sort: sort,
                    aggs: {
                        sites: {
                            terms: { field: 'SITE', size: 10 }
                        },
                        clusters: {
                            terms: { field: 'CLUSTERID', size: 10 }
                        },
                        users: {
                            terms: { field: 'USERNAME', size: 10 }
                        }
                    }
                }
            });

            const hits = response.body?.hits || response.hits || { total: { value: 0 }, hits: [] };
            const totalValue = hits.total?.value || hits.total || 0;

            res.json({
                total: { value: totalValue, relation: hits.total?.relation || "eq" },
                hits: hits.hits?.map(hit => ({
                    id: hit._id,
                    score: hit._score,
                    source: hit._source
                })) || [],
                aggregations: response.body?.aggregations || response.aggregations || {}
            });

        } catch (error) {
            logger.error('Advanced search error:', error);
            res.status(500).json({ error: 'Advanced search failed', message: error.message });
        }
    },

    /**
     * Get document by ID
     */
    async getById(req, res) {
        try {
            const { id } = req.params;

            if (!id) {
                return res.status(400).json({ error: 'Document ID is required' });
            }

            const esClient = getClient();
            const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

            const response = await esClient.get({
                index: indexName,
                id: id
            });

            const source = response.body?._source || response._source;

            res.json({
                id: response.body?._id || response._id,
                source: source
            });

        } catch (error) {
            if (error.meta?.statusCode === 404) {
                return res.status(404).json({ error: 'Document not found' });
            }
            logger.error('Get by ID error:', error);
            res.status(500).json({ error: 'Failed to retrieve document', message: error.message });
        }
    }
};

module.exports = searchController;
