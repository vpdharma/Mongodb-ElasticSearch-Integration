const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const dotenv = require('dotenv');
const { connectMongoDB } = require('./services/mongoService');
const { connectElasticsearch } = require('./services/elasticsearchService');
const { setupChangeStreams } = require('./services/syncService');
const logger = require('./utils/logger');

// Load environment variables
dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(helmet());
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Request logging middleware
app.use((req, res, next) => {
    logger.info(`${req.method} ${req.path} - ${req.ip}`);
    next();
});

// Routes
app.use('/api/search', require('./routes/searchRoutes'));
app.use('/api/sync', require('./routes/syncRoutes'));

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({
        status: 'OK',
        timestamp: new Date().toISOString(),
        uptime: process.uptime()
    });
});

// Error handling middleware
app.use((err, req, res, next) => {
    logger.error('Unhandled error:', err);
    res.status(500).json({
        error: 'Internal server error',
        message: process.env.NODE_ENV === 'development' ? err.message : 'Something went wrong'
    });
});

// 404 handler - FIXED for Express 5.x
// OLD: app.use('*', (req, res) => {
// NEW: Use named parameter for wildcard
app.use('/{*catchAll}', (req, res) => {
    res.status(404).json({
        error: 'Route not found',
        path: req.path
    });
});

// Initialize connections and start server
async function startServer() {
    try {
        await connectMongoDB();
        logger.info('Connected to MongoDB');

        await connectElasticsearch();
        logger.info('Connected to Elasticsearch');

        await setupChangeStreams();
        logger.info('MongoDB Change Streams initialized');

        app.listen(PORT, () => {
            logger.info(`🚀 Server running on http://localhost:${PORT}`);
            logger.info(`📊 Health check: http://localhost:${PORT}/health`);
            console.log(`✅ Server started successfully on port ${PORT}`);
        });

    } catch (error) {
        logger.error('Failed to start server:', error);
        console.error('❌ Server startup failed:', error.message);
        process.exit(1);
    }
}

startServer();
