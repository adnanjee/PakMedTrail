const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const winston = require('winston');
const fabricHelper = require('./fabric/fabricHelper');
require('dotenv').config();

// Import route handlers
const apiTransferRoutes = require('./routes/apiTransfer');
const manufacturingRoutes = require('./routes/manufacturing');
const queryRoutes = require('./routes/queries');

const app = express();
const PORT = process.env.PORT || 3000;

// Configure logger
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.json()
  ),
  transports: [
    new winston.transports.File({ filename: 'error.log', level: 'error' }),
    new winston.transports.File({ filename: 'combined.log' }),
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      )
    })
  ]
});

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Request logging middleware
app.use((req, res, next) => {
  logger.info(`${req.method} ${req.path}`, {
    ip: req.ip,
    userAgent: req.get('User-Agent')
  });
  next();
});

// Initialize wallet on startup
let walletInitialized = false;
app.use(async (req, res, next) => {
  if (!walletInitialized) {
    try {
      await fabricHelper.initializeWallet();
      walletInitialized = true;
      logger.info('Wallet initialized successfully');
    } catch (error) {
      logger.error('Wallet initialization failed:', error);
      return res.status(500).json({
        error: 'Wallet initialization failed',
        message: error.message
      });
    }
  }
  next();
});

// Health check with wallet status
app.get('/health', async (req, res) => {
  try {
    const identities = await fabricHelper.listIdentities();
    res.status(200).json({
      status: 'OK',
      timestamp: new Date().toISOString(),
      service: 'Fabric Drug Supply Chain API',
      wallet: {
        initialized: true,
        identities: identities
      }
    });
  } catch (error) {
    res.status(500).json({
      status: 'ERROR',
      error: error.message
    });
  }
});

// List available identities
app.get('/identities', async (req, res) => {
  try {
    const identities = await fabricHelper.listIdentities();
    res.status(200).json({
      success: true,
      identities: identities
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// API Routes
app.use('/api/raw-materials', apiTransferRoutes);
app.use('/api/drugs', manufacturingRoutes);
app.use('/api/query', queryRoutes);

// Error handling middleware
app.use((error, req, res, next) => {
  logger.error('Unhandled error:', error);
  res.status(500).json({
    error: 'Internal server error',
    message: error.message
  });
});

// 404 handler
app.use('*', (req, res) => {
  res.status(404).json({
    error: 'Endpoint not found',
    path: req.originalUrl
  });
});

// Start server
app.listen(PORT, () => {
  logger.info(`Server running on port ${PORT}`);
  console.log(`Fabric Drug Supply Chain API Server running on port ${PORT}`);
});

module.exports = app;