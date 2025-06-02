#!/usr/bin/env node

const dotenv = require('dotenv');
dotenv.config();

const { connectMongoDB } = require('../src/services/mongoService');
const { connectElasticsearch } = require('../src/services/elasticsearchService');
const { bulkSyncToElasticsearch } = require('../src/services/syncService');
const logger = require('../src/utils/logger');

async function runSync() {
    try {
        console.log('🚀 Starting MongoDB to Elasticsearch sync...\n');

        // Connect to databases
        await connectMongoDB();
        console.log('✅ Connected to MongoDB');

        await connectElasticsearch();
        console.log('✅ Connected to Elasticsearch');

        // Run the sync
        const totalProcessed = await bulkSyncToElasticsearch();

        console.log(`\n🎉 Sync completed successfully!`);
        console.log(`📊 Total documents processed: ${totalProcessed}`);

        process.exit(0);
    } catch (error) {
        console.error('❌ Sync failed:', error);
        logger.error('Sync script failed:', error);
        process.exit(1);
    }
}

// Handle script termination
process.on('SIGINT', () => {
    console.log('\n🛑 Sync interrupted by user');
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\n🛑 Sync terminated');
    process.exit(0);
});

runSync();
