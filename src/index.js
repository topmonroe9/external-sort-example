const fs = require("fs");
const path = require("path");
const { Splitter } = require("./splitter");
const { Sorter } = require("./sorter");
const { Merger } = require("./merger");
const { createTempDir, cleanupTempDir, formatBytes } = require("./utils");

/**
 * External sort for large files
 * @param {string} inputFile
 * @param {string} outputFile
 * @param {Object} options
 * @param {number} options.maxMemoryBytes - Max mem usage (in bytes)
 * @param {string} options.tempDir
 * @param {boolean} options.cleanup - Need temp files cleanup?
 * @returns {Promise<void>}
 */
async function externalSort(inputFile, outputFile, options = {}) {
  const defaultOptions = {
    maxMemoryBytes: 400 * 1024 * 1024, // 400MB (keeping some memory for node)
    tempDir: path.join(process.cwd(), "temp_sort"),
    cleanup: true,
  };

  const opts = { ...defaultOptions, ...options };
  console.log(`Starting external sort of ${inputFile}`);
  console.log(`Using up to ${formatBytes(opts.maxMemoryBytes)} of memory`);

  const startTime = Date.now();

  try {
    const tempDir = await createTempDir(opts.tempDir);
    console.log(`Created temp directory: ${tempDir}`);

    console.log("Step 1: Splitting file into chunks...");
    const splitter = new Splitter({ maxMemoryBytes: opts.maxMemoryBytes });
    const chunkFiles = await splitter.split(inputFile, tempDir);
    console.log(`Created ${chunkFiles.length} chunks`);

    console.log("Step 2: Sorting individual chunks...");
    const sorter = new Sorter();
    const sortedChunks = await sorter.sortChunks(chunkFiles);
    console.log(`Sorted ${sortedChunks.length} chunks`);

    console.log("Step 3: Merging sorted chunks...");
    const merger = new Merger();
    await merger.merge(sortedChunks, outputFile);
    console.log(`Merged all chunks to ${outputFile}`);

    if (opts.cleanup) {
      console.log("Cleaning up temporary files...");
      await cleanupTempDir(tempDir);
      console.log("Cleanup complete");
    }

    const elapsedTime = (Date.now() - startTime) / 1000;
    console.log(`External sort completed in ${elapsedTime.toFixed(2)} seconds`);
  } catch (error) {
    console.error("Error during external sort:", error);
    throw error;
  }
}

module.exports = { externalSort };

if (require.main === module) {
  const args = process.argv.slice(2);

  if (args.length < 2) {
    console.error(
      "Usage: node index.js <input-file> <output-file> [max-memory-mb]"
    );
    process.exit(1);
  }

  const inputFile = args[0];
  const outputFile = args[1];
  const maxMemoryMB = args[2] ? parseInt(args[2], 10) : 400;

  externalSort(inputFile, outputFile, {
    maxMemoryBytes: maxMemoryMB * 1024 * 1024,
  }).catch((err) => {
    console.error("External sort failed:", err);
    process.exit(1);
  });
}
