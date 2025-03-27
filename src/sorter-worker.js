// sorter-worker.js - worker for chunk sorting
const fs = require("fs");
const { parentPort, workerData } = require("worker_threads");

/**
 * sort a chunk file
 * @param {string} chunkPath - path to the chunk file
 * @returns {Promise<string>} - path to the sorted file
 */
async function sortChunk(chunkPath) {
  try {
    // read file
    const data = fs.readFileSync(chunkPath, "utf-8");

    // split into lines and filter empty lines
    const lines = data.split("\n");
    const filteredLines = lines.filter((line) => line.trim().length > 0);

    // case-sensitive sorting
    const sortedLines = filteredLines.sort((a, b) => {
      if (a < b) return -1;
      if (a > b) return 1;
      return 0;
    });

    // write sorted lines to a new file
    const sortedChunkPath = chunkPath.replace(".txt", ".sorted.txt");
    fs.writeFileSync(sortedChunkPath, sortedLines.join("\n") + "\n");

    // try to delete original file
    try {
      fs.unlinkSync(chunkPath);
    } catch (err) {
      console.warn(
        "warning: could not delete original chunk file " +
          chunkPath +
          ": " +
          err.message
      );
    }

    return sortedChunkPath;
  } catch (error) {
    throw new Error("error in worker: " + error.message);
  }
}

// get data from main thread
const { chunkPath } = workerData;

// perform sorting
sortChunk(chunkPath)
  .then((sortedChunkPath) => {
    // send result back to main thread
    parentPort.postMessage({
      success: true,
      sortedChunkPath,
    });
  })
  .catch((error) => {
    // send error to main thread
    parentPort.postMessage({
      success: false,
      error: error.message,
    });
  });
