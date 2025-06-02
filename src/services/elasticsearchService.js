const { Client } = require('@elastic/elasticsearch');
const logger = require('../utils/logger');

let esClient = null;

const connectElasticsearch = async () => {
    try {
        const config = {
            node: process.env.ELASTICSEARCH_NODE || 'http://localhost:9200',
        };

        if (process.env.ELASTICSEARCH_USERNAME && process.env.ELASTICSEARCH_PASSWORD) {
            config.auth = {
                username: process.env.ELASTICSEARCH_USERNAME,
                password: process.env.ELASTICSEARCH_PASSWORD
            };
        }

        esClient = new Client(config);

        // Test connection
        const health = await esClient.cluster.health();
        logger.info('Elasticsearch cluster health:', health.status);

        // Create index if it doesn't exist
        await createIndexIfNotExists();

        return esClient;
    } catch (error) {
        logger.error('Elasticsearch connection error:', error);
        throw error;
    }
};

const createIndexIfNotExists = async () => {
    const indexName = process.env.ELASTICSEARCH_INDEX || 'subnet_search';

    try {
        const exists = await esClient.indices.exists({ index: indexName });

        if (!exists) {
            await esClient.indices.create({
                index: indexName,
                body: {
                    settings: {
                        analysis: {
                            analyzer: {
                                autocomplete_analyzer: {
                                    type: 'custom',
                                    tokenizer: 'standard',
                                    filter: ['lowercase', 'autocomplete_filter']
                                }
                            },
                            filter: {
                                autocomplete_filter: {
                                    type: 'edge_ngram',
                                    min_gram: 1,
                                    max_gram: 20
                                }
                            }
                        }
                    },
                    mappings: {
                        properties: {
                            CLUSTERID: {
                                type: 'keyword',
                                fields: {
                                    autocomplete: {
                                        type: 'text',
                                        analyzer: 'autocomplete_analyzer'
                                    }
                                }
                            },
                            CIDR: { type: 'keyword' },
                            CIDRIPV4: { type: 'keyword' },
                            IPV4: { type: 'ip' },
                            IP: { type: 'ip' },
                            SITE: {
                                type: 'keyword',
                                fields: {
                                    autocomplete: {
                                        type: 'text',
                                        analyzer: 'autocomplete_analyzer'
                                    }
                                }
                            },
                            DESCRIPTION: {
                                type: 'text',
                                fields: {
                                    autocomplete: {
                                        type: 'text',
                                        analyzer: 'autocomplete_analyzer'
                                    }
                                }
                            },
                            TIMESTAMP: { type: 'date' },
                            USERNAME: { type: 'keyword' },
                            suggest: {
                                type: 'completion'
                            }
                        }
                    }
                }
            });

            logger.info(`Created Elasticsearch index: ${indexName}`);
        }
    } catch (error) {
        logger.error('Error creating Elasticsearch index:', error);
        throw error;
    }
};

const getClient = () => {
    if (!esClient) {
        throw new Error('Elasticsearch not connected. Call connectElasticsearch() first.');
    }
    return esClient;
};

// IMPORTANT: Make sure all functions are properly exported
module.exports = {
    connectElasticsearch,  // This was missing or incorrectly exported
    getClient,
    createIndexIfNotExists
};
