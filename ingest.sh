#! /bin/bash
npm run build
node build/ingest.js --format $1