require('dotenv').config();
const express = require('express');
const AWS = require('aws-sdk');
const Redis = require('ioredis');
const promClient = require('prom-client');
const path = require('path');

const app = express();
const port = process.env.PORT || 8080;
const region = process.env.AWS_REGION || 'unknown';

// AWS S3 Setup
AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: region,
});
const s3 = new AWS.S3();

// Redis Setup
const redis = new Redis({
  host: process.env.REDIS_HOST,
  port: process.env.REDIS_PORT,
});
redis.on('error', (err) => console.log('Redis error:', err.message));

// Prometheus setup
promClient.collectDefaultMetrics();

// Application Metrics
// File popularity (total views)
const fileViews = new promClient.Counter({
  name: 'file_view_total',
  help: 'Total number of file views',
  labelNames: ['filename', 'category', 'region'],
});

// Cache efficiency
const cacheHits = new promClient.Counter({
  name: 'cache_hits_total',
  help: 'Number of cache hits (Redis)',
  labelNames: ['filename', 'category', 'region'],
});

const cacheMisses = new promClient.Counter({
  name: 'cache_misses_total',
  help: 'Number of cache misses (Redis)',
  labelNames: ['filename', 'category', 'region'],
});

// Latency (per file request)
const requestDuration = new promClient.Histogram({
  name: 'file_request_duration_seconds',
  help: 'Duration of file requests (seconds)',
  labelNames: ['filename', 'category', 'region', 'cache_status'],
  buckets: [0.01, 0.05, 0.1, 0.3, 0.5, 1, 2, 5], // define latency buckets
});

// Serve static UI
app.use(express.static(path.join(__dirname, 'public')));


// List all files in S3 bucket grouped by category
app.get('/files', async (req, res) => {
  try {
    const data = await s3.listObjectsV2({ Bucket: process.env.S3_BUCKET_NAME }).promise();

    const filesByCategory = {};
    data.Contents.forEach((item) => {
      const parts = item.Key.split('/');
      if (item.Key.endsWith('/') || (parts.length === 1 && !parts[0])) return;

      if (parts.length > 1) {
        const category = parts[0];
        const filename = parts[1];
        if (filename && filename.trim() !== '') {
          if (!filesByCategory[category]) filesByCategory[category] = [];
          filesByCategory[category].push(filename);
        }
      } else {
        if (!filesByCategory['Uncategorized']) filesByCategory['Uncategorized'] = [];
        filesByCategory['Uncategorized'].push(item.Key);
      }
    });

    res.json({ filesByCategory });
  } catch (err) {
    console.error(err);
    res.status(500).send('Error listing files');
  }
});

// View a specific file
app.get('/view/:category/:filename', async (req, res) => {
  const { category, filename } = req.params;
  const fullKey = `${category}/${filename}`;

  const endTimer = requestDuration.startTimer({ filename, category, region });
  let cacheStatus = 'miss';

  try {
    // Try Redis Cache
    const cached = await redis.get(fullKey);
    if (cached) {
      cacheHits.labels(filename, category, region).inc();
      fileViews.labels(filename, category, region).inc();
      cacheStatus = 'hit';
      endTimer({ filename, category, region, cache_status: cacheStatus });
      console.log(`Cache hit: ${fullKey}`);
      return res.send(Buffer.from(cached, 'base64'));
    }

    // Fetch from S3 if not in cache
    const params = { Bucket: process.env.S3_BUCKET_NAME, Key: fullKey };
    const data = await s3.getObject(params).promise();

    // Store in Redis
    try {
      await redis.set(fullKey, data.Body.toString('base64'), 'EX', 3600); // TTL = 1h
    } catch (err) {
      console.log('Redis set error:', err.message);
    }

    cacheMisses.labels(filename, category, region).inc();
    fileViews.labels(filename, category, region).inc();
    res.set('Content-Type', data.ContentType);
    res.send(data.Body);

    endTimer({ filename, category, region, cache_status: cacheStatus });
  } catch (err) {
    console.error(err);
    res.status(500).send('Error viewing file');
  }
});

// Prometheus metrics endpoint
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', promClient.register.contentType);
  res.end(await promClient.register.metrics());
});



app.listen(port, () => {
  console.log(`Server running in region ${region} on port ${port}`);
});


