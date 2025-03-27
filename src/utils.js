// utils.js - helper utilities
const fs = require("fs");
const path = require("path");
const { promisify } = require("util");

/**
 * format bytes to human-readable string
 * @param {number} bytes - number of bytes
 * @param {number} decimals - number of decimal places
 * @returns {string} - formatted string
 */
function formatBytes(bytes, decimals = 2) {
  if (bytes === 0) return "0 Bytes";

  const k = 1024;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return (
    parseFloat((bytes / Math.pow(k, i)).toFixed(decimals)) + " " + sizes[i]
  );
}

/**
 * create a temporary directory for sort operations
 * @param {string} tempDir - directory path
 * @returns {Promise<string>} - path to the created directory
 */
async function createTempDir(tempDir) {
  try {
    await promisify(fs.mkdir)(tempDir, { recursive: true });
    return tempDir;
  } catch (error) {
    throw new Error(`failed to create temp directory: ${error.message}`);
  }
}

/**
 * clean up temporary directory and files
 * @param {string} tempDir - directory path
 * @returns {Promise<void>}
 */
async function cleanupTempDir(tempDir) {
  try {
    const files = await promisify(fs.readdir)(tempDir);

    for (const file of files) {
      await promisify(fs.unlink)(path.join(tempDir, file)).catch(() => {
        // ignoring individual file errors
      });
    }

    await promisify(fs.rmdir)(tempDir);
  } catch (error) {
    console.warn(
      `warning: failed to clean up temp directory: ${error.message}`
    );
  }
}

/**
 * estimate memory usage by the Node.js process
 * @returns {number} - memory usage in bytes
 */
function estimateMemoryUsage() {
  const memoryUsage = process.memoryUsage();
  return memoryUsage.heapUsed + memoryUsage.external;
}

/**
 * sleep for a specified duration
 * @param {number} ms - duration in milliseconds
 * @returns {Promise<void>}
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * create a test file with random lines
 * @param {string} filePath - path to the file
 * @param {number} sizeInMB - size of the file in MB
 * @param {number} avgLineLength - average length of each line
 * @returns {Promise<void>}
 */
async function createTestFile(filePath, sizeInMB, avgLineLength = 100) {
  const targetSizeBytes = sizeInMB * 1024 * 1024;
  const writeStream = fs.createWriteStream(filePath);

  let bytesWritten = 0;
  const progressInterval = Math.max(1, Math.floor(sizeInMB / 10));
  let lastProgressMB = 0;

  const characters =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 ";

  try {
    while (bytesWritten < targetSizeBytes) {
      // generate a random line with some variation in length
      const lineLength = Math.max(
        1,
        Math.floor(avgLineLength * (0.5 + Math.random()))
      );
      let line = "";

      for (let i = 0; i < lineLength; i++) {
        line += characters.charAt(
          Math.floor(Math.random() * characters.length)
        );
      }

      line += "\n";

      writeStream.write(line);
      bytesWritten += Buffer.byteLength(line);

      const mbWritten = Math.floor(bytesWritten / (1024 * 1024));
      if (mbWritten >= lastProgressMB + progressInterval) {
        console.log(`Created ${mbWritten}MB / ${sizeInMB}MB...`);
        lastProgressMB = mbWritten;
      }
    }

    await new Promise((resolve, reject) => {
      writeStream.end((err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    console.log(
      `test file created: ${filePath} (${formatBytes(bytesWritten)})`
    );
  } catch (error) {
    writeStream.destroy();
    throw error;
  }
}

/**
 * verify that a file is correctly sorted
 * @param {string} filePath - path to the file
 * @returns {Promise<boolean>} - true if file is sorted
 */
async function verifySortedFile(filePath) {
  const fileStream = fs.createReadStream(filePath);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  let previousLine = null;
  let lineCount = 0;
  let isSorted = true;

  for await (const line of rl) {
    lineCount++;

    if (previousLine !== null && line.localeCompare(previousLine) < 0) {
      console.error(`file not sorted at line ${lineCount}:`);
      console.error(`previous: "${previousLine}"`);
      console.error(`current: "${line}"`);
      isSorted = false;
      break;
    }

    previousLine = line;

    if (lineCount % 1000000 === 0) {
      console.log(`verified ${lineCount.toLocaleString()} lines...`);
    }
  }

  console.log(`verified ${lineCount.toLocaleString()} lines total`);
  return isSorted;
}

module.exports = {
  formatBytes,
  createTempDir,
  cleanupTempDir,
  estimateMemoryUsage,
  sleep,
  createTestFile,
  verifySortedFile,
};
