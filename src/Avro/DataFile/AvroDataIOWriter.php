<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Avro\DataFile;

use Avro\Datum\AvroIOBinaryEncoder;
use Avro\Datum\AvroIODatumReader;
use Avro\Datum\AvroIODatumWriter;
use Avro\Datum\IOBinaryEncoderInterface;
use Avro\Exception\AvroException;
use Avro\Exception\AvroNotImplementedException;
use Avro\Exception\Data\AvroDataIOException;
use Avro\Exception\Datum\AvroIOTypeException;
use Avro\Exception\IO\AvroIOException;
use Avro\Exception\Schema\AvroSchemaParseException;
use Avro\IO\AbstractAvroIO;
use Avro\IO\IOInterface;
use Avro\IO\AvroStringIO;
use Avro\Schema\AvroSchema;
use Avro\Schema\AvroSchemaInterface;

/**
 * Class AvroDataIOWriter
 *
 * Writes Avro Data to an AvroIO source using an AvroSchema
 */
class AvroDataIOWriter implements DataIOWriterInterface
{
    /** @var string */
    protected $syncMarker;

    /** @var IOInterface object container where Data is written */
    private $io;

    /** @var IOBinaryEncoderInterface encoder for object container */
    private $encoder;

    /** @var AvroIODatumWriter */
    private $datumWriter;

    /** @var AvroStringIO buffer for writing */
    private $buffer;

    /** @var IOBinaryEncoderInterface encoder for buffer */
    private $bufferEncoder;

    /** * @var int count of items written to block */
    private $blockCount;

    /** * @var array map of object container metadata */
    private $metadata;

    /**
     * @param IOInterface         $io
     * @param AvroIODatumWriter   $datumWriter
     * @param AvroSchemaInterface $writersSchema
     *
     * @throws AvroDataIOException
     * @throws AvroException
     * @throws AvroIOException
     * @throws AvroIOTypeException
     * @throws AvroNotImplementedException
     * @throws AvroSchemaParseException
     */
    public function __construct($io, $datumWriter, $writersSchema = null)
    {
        if (!($io instanceof AbstractAvroIO)) {
            throw new AvroDataIOException('io must be instance of AvroIO');
        }

        $this->io            = $io;
        $this->encoder       = new AvroIOBinaryEncoder($this->io);
        $this->datumWriter   = $datumWriter;
        $this->buffer        = new AvroStringIO();
        $this->bufferEncoder = new AvroIOBinaryEncoder($this->buffer);
        $this->blockCount    = 0;
        $this->metadata      = [];

        if ($writersSchema) {
            $this->syncMarker = self::generateSyncMarker();

            $this->metadata[AvroDataIO::METADATA_CODEC_ATTR]  = AvroDataIO::NULL_CODEC;
            $this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR] = strval($writersSchema);
            $this->writeHeader();
        } else {
            $dfr              = new AvroDataIOReader($this->io, new AvroIODatumReader());
            $this->syncMarker = $dfr->syncMarker;

            $this->metadata[AvroDataIO::METADATA_CODEC_ATTR] = $dfr->metadata[AvroDataIO::METADATA_CODEC_ATTR];

            $schemaFromFile                                   = $dfr->metadata[AvroDataIO::METADATA_SCHEMA_ATTR];
            $this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR] = $schemaFromFile;
            $this->datumWriter->setWritersSchema(AvroSchema::parse($schemaFromFile));
            $this->seek(0, SEEK_END);
        }
    }

    /**
     * @return string a new, unique sync marker.
     */
    private static function generateSyncMarker()
    {
        // From http://php.net/manual/en/function.mt-rand.php comments
        return pack(
            'S8',
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff) | 0x4000,
            mt_rand(0, 0xffff) | 0x8000,
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff)
        );
    }

    /**
     * Flushes biffer to AvroIO object container.
     *
     * @return mixed value of $io->flush()
     * @throws AvroDataIOException
     * @throws AvroIOException
     * @throws AvroNotImplementedException
     * @see IOInterface::flush()
     */
    private function flush()
    {
        $this->writeBlock();

        return $this->io->flush();
    }

    /**
     * Writes a block of Data to the Avro\IO\AvroIO object container.
     *
     * @throws AvroDataIOException if the codec provided by the encoder is not supported
     * @throws AvroIOException
     * @throws AvroNotImplementedException
     * @internal Should the codec check happen in the constructor? Why wait until we're writing Data?
     */
    private function writeBlock()
    {
        if ($this->blockCount > 0) {
            $this->encoder->writeLong($this->blockCount);
            $to_write = strval($this->buffer);
            $this->encoder->writeLong(strlen($to_write));

            if (AvroDataIO::isValidCodec($this->metadata[AvroDataIO::METADATA_CODEC_ATTR])) {
                $this->write($to_write);
            } else {
                throw new AvroDataIOException(sprintf(
                    'codec %s is not supported',
                    $this->metadata[AvroDataIO::METADATA_CODEC_ATTR]
                ));
            }

            $this->write($this->syncMarker);
            $this->buffer->truncate();
            $this->blockCount = 0;
        }
    }

    /**
     * Writes the header of the Avro\IO\AvroIO object container
     * @throws AvroNotImplementedException
     * @throws AvroException
     * @throws AvroIOTypeException
     * @throws AvroSchemaParseException
     */
    private function writeHeader()
    {
        $this->write(AvroDataIO::magic());
        $this->datumWriter->writeData(AvroDataIO::metadatSchema(), $this->metadata, $this->encoder);
        $this->write($this->syncMarker);
    }

    /**
     * @param string $bytes
     *
     * @return int
     * @throws AvroNotImplementedException
     * @uses IOInterface::write()
     */
    private function write($bytes)
    {
        return $this->io->write($bytes);
    }

    /**
     * @param int $offset
     * @param int $whence
     *
     * @return bool
     * @throws AvroNotImplementedException
     * @uses IOInterface::seek()
     */
    private function seek($offset, $whence)
    {
        return $this->io->seek($offset, $whence);
    }

    /**
     * @param mixed $datum
     *
     * @throws AvroDataIOException
     * @throws AvroException
     * @throws AvroIOException
     * @throws AvroIOTypeException
     * @throws AvroNotImplementedException
     * @throws AvroSchemaParseException
     */
    public function append($datum)
    {
        $this->datumWriter->write($datum, $this->bufferEncoder);
        $this->blockCount++;

        if ($this->buffer->length() >= AvroDataIO::SYNC_INTERVAL) {
            $this->writeBlock();
        }
    }

    /**
     * Flushes buffer to Avro\IO\AvroIO object container and closes it.
     *
     * @return mixed value of $io->close()
     * @throws AvroDataIOException
     * @throws AvroNotImplementedException
     * @throws AvroIOException
     * @see IOInterface::close()
     */
    public function close()
    {
        $this->flush();

        return $this->io->close();
    }
}
