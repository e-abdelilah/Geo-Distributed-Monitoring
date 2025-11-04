require('dotenv').config();
const express = require('express');
const AWS = require('aws-sdk');
const Redis = require('ioredis');
const promClient = require('prom-client');
const path = require('path');

const app = express();
const port = process.env.PORT || 8080;

//  AWS S3 Setup
AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION,
});
const s3 = new AWS.S3();

//Redis Setup
const redis = new Redis({
  host: process.env.REDIS_HOST,
  port: process.env.REDIS_PORT,
});
redis.on('error', (err) => console.log('Redis error:', err.message));

// Prometheus Metrics 
const collectDefaultMetrics = promClient.collectDefaultMetrics;
collectDefaultMetrics();

const fileDownloads = new promClient.Counter({
  name: 'file_download_total',
  help: 'Total number of file downloads',
  labelNames: ['filename'],
});

//  Serve static UI 
app.use(express.static(path.join(__dirname, 'public')));

//  Routes 
// List all files in S3 bucket
app.get('/files', async (req, res) => {
  try {
    const data = await s3.listObjectsV2({ Bucket: process.env.S3_BUCKET_NAME }).promise();
    const files = data.Contents.map((item) => item.Key);
    res.json({ files });
  } catch (err) {
    console.error(err);
    res.status(500).send('Error listing files');
  }
});

// Download a file
app.get('/download/:filename', async (req, res) => {
  const filename = req.params.filename;

  try {
    // Check cache first
    const cached = await redis.get(filename);
    if (cached) {
      console.log('Cache hit for', filename);
      fileDownloads.labels(filename).inc();
      return res.send(Buffer.from(cached, 'base64'));
    }

    // Fetch from S3
    const params = { Bucket: process.env.S3_S3_BUCKET_NAME, Key: filename };
    const data = await s3.getObject(params).promise();

    // Save in Redis cache
    try {
      await redis.set(filename, data.Body.toString('base64'), 'EX', 3600); // 1 hour expiry
    } catch (err) {
      console.log('Redis set error:', err.message);
    }

    fileDownloads.labels(filename).inc();
    res.set('Content-Type', data.ContentType);
    res.send(data.Body);
  } catch (err) {
    console.error(err);
    res.status(500).send('Error downloading file');
  }
});

// Prometheus metrics endpoint
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', promClient.register.contentType);
  res.end(await promClient.register.metrics());
});

// Start server
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
