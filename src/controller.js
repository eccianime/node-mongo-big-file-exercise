const Records = require('./records.model');
const fs = require('fs');
const csv = require('csv-parser');

const deleteTemporaryFile = (filePath) => {
  fs.unlink(filePath, (err) => {
    if (err) {
      console.error('Error al eliminar el archivo temporal:', err);
    }
  });
};

const logMemoryUsage = (label, memory) => {
  console.log(
    `${label}: RSS: ${(memory.rss / (1024 * 1024)).toFixed(
      2
    )} MB, Heap Used: ${(memory.heapUsed / (1024 * 1024)).toFixed(2)} MB`
  );
};

const upload = async (req, res) => {
  console.time('csv_processing_time');
  const { file } = req;

  const startMemory = process.memoryUsage();
  logMemoryUsage('Memoria al inicio', startMemory);

  if (!file) {
    return res.status(400).json({ message: 'No se ha subido ningún archivo.' });
  }

  const recordsToInsert = [];
  const batchSize = 1000;

  try {
    fs.createReadStream(file.path)
      .pipe(csv())
      .on('data', async function (row) {
        recordsToInsert.push(row);

        if (recordsToInsert.length >= batchSize) {
          this.pause();
          await Records.insertMany(recordsToInsert);
          recordsToInsert.length = 0;
          this.resume();
        }
      })
      .on('end', async () => {
        if (recordsToInsert.length > 0) {
          await Records.insertMany(recordsToInsert);
        }

        deleteTemporaryFile(file.path);

        console.timeEnd('csv_processing_time');
        const endMemory = process.memoryUsage();
        logMemoryUsage('Memoria al final', endMemory);
        return res.status(200).json({
          message: 'Archivo procesado y registros guardados exitosamente.',
        });
      })
      .on('error', (err) => {
        console.error('Error al procesar el archivo CSV:', err);
        deleteTemporaryFile(file.path);
        console.timeEnd('csv_processing_time');
        const errorMemory = process.memoryUsage();
        logMemoryUsage('Memoria al error', errorMemory);
        return res.status(500).json({
          message: 'Error al procesar el archivo.',
          error: err.message,
        });
      });
  } catch (err) {
    console.error('Error general en la función upload:', err);
    deleteTemporaryFile(file.path);
    console.timeEnd('csv_processing_time');
    const catchMemory = process.memoryUsage();
    logMemoryUsage('Memoria en catch', catchMemory);
    return res
      .status(500)
      .json({ message: 'Error interno del servidor.', error: err.message });
  }
};

const list = async (_, res) => {
  try {
    const data = await Records.find({}).limit(10).lean();

    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json(err);
  }
};

module.exports = {
  upload,
  list,
};
