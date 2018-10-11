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

use Avro\Datum\AvroIOBinaryDecoder;
use Avro\Datum\IOBinaryDecoderInterface;
use Avro\Datum\IODatumReaderInterface;
use Avro\Exception\AvroException;
use Avro\Exception\AvroNotImplementedException;
use Avro\Exception\Data\AvroDataIOException;
use Avro\Exception\Datum\AvroIOSchemaMatchException;
use Avro\Exception\Schema\AvroSchemaParseException;
use Avro\IO\AbstractAvroIO;
use Avro\IO\IOInterface;
use Avro\Schema\AvroSchema;
use Avro\Util\AvroUtil;

/**
 * Class AvroDataIOReader
 *
 * Reads Avro Data from an AvroIO source using an AvroSchema.
 */
class AvroDataIOReader implements DataIOReaderInterface
{
    /** @var string */
    public $syncMarker;

    /** @var array object container metadata */
    public $metadata;

    /** @var IOInterface */
    private $io;

    /** @var IOBinaryDecoderInterface */
    private $decoder;

    /** @var IODatumReaderInterface */
    private $datumReader;

    /** @var int count of items in block */
    private $blockCount;

    /**
     * AvroDataIOReader constructor.
     *
     * @param IOInterface            $io          source from which to read
     * @param IODatumReaderInterface $datumReader reader that understands the Data schema
     *
     * @throws AvroDataIOException
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     * @throws AvroNotImplementedException
     * @throws AvroSchemaParseException
     *
     * @uses readHeader()
     */
    public function __construct($io, $datumReader)
    {
        if (!($io instanceof AbstractAvroIO)) {
            throw new AvroDataIOException('io must be instance of Avro\IO\AvroIO');
        }

        $this->io          = $io;
        $this->decoder     = new AvroIOBinaryDecoder($this->io);
        $this->datumReader = $datumReader;
        $this->readHeader();

        $codec = AvroUtil::arrayValue($this->metadata, AvroDataIO::METADATA_CODEC_ATTR);
        if ($codec && !AvroDataIO::isValidCodec($codec)) {
            throw new AvroDataIOException(sprintf('Uknown codec: %s', $codec));
        }

        $this->blockCount = 0;
        // FIXME: Seems unsanitary to set writers_schema here.
        // Can't constructor take it as an argument?
        $this->datumReader->setWritersSchema(AvroSchema::parse($this->metadata[AvroDataIO::METADATA_SCHEMA_ATTR]));
    }

    /**
     * Reads header of object container
     *
     * @throws AvroDataIOException If the file is not an Avro Data file.
     * @throws AvroNotImplementedException
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    private function readHeader()
    {
        $this->seek(0, AbstractAvroIO::SEEK_SET);

        $magic = $this->read(AvroDataIO::magicSize());

        if (strlen($magic) < AvroDataIO::magicSize()) {
            throw new AvroDataIOException('Not an Avro Data file: shorter than the Avro magic block');
        }

        if (AvroDataIO::magic() != $magic) {
            throw new AvroDataIOException(sprintf(
                'Not an Avro Data file: %s does not match %s',
                $magic,
                AvroDataIO::magic()
            ));
        }

        $this->metadata   = $this->datumReader->readData(
            AvroDataIO::metadatSchema(),
            AvroDataIO::metadatSchema(),
            $this->decoder
        );
        $this->syncMarker = $this->read(AvroDataIO::SYNC_SIZE);
    }

    /**
     * @uses IOInterface::seek()
     *
     * @param $offset
     * @param $whence
     *
     * @return bool
     *
     * @throws AvroNotImplementedException
     */
    private function seek($offset, $whence)
    {
        return $this->io->seek($offset, $whence);
    }

    /**
     * @uses IOInterface::read()
     *
     * @param $len
     *
     * @return string
     *
     * @throws AvroNotImplementedException
     */
    private function read($len)
    {
        return $this->io->read($len);
    }

    /**
     * @uses IOInterface::isEof()
     *
     * @throws AvroNotImplementedException
     */
    private function isEof()
    {
        return $this->io->isEof();
    }

    /**
     * @return bool
     *
     * @throws AvroNotImplementedException
     */
    private function skipSync()
    {
        $proposed_sync_marker = $this->read(AvroDataIO::SYNC_SIZE);
        if ($proposed_sync_marker != $this->syncMarker) {
            $this->seek(-AvroDataIO::SYNC_SIZE, AbstractAvroIO::SEEK_CUR);

            return false;
        }

        return true;
    }

    /**
     * Reads the block header (which includes the count of items in the block and the length in bytes of the block)
     *
     * @return int length in bytes of the block.
     *
     * @throws AvroNotImplementedException
     */
    private function readBlockHeader()
    {
        $this->blockCount = $this->decoder->readLong();

        return $this->decoder->readLong();
    }

    /**
     * @internal Would be nice to implement Data() as an iterator, I think
     *
     * @return array of Data from object container.
     *
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     * @throws AvroNotImplementedException
     */
    public function data()
    {
        $data = [];
        while (true) {
            if (0 == $this->blockCount) {
                if ($this->isEof()) {
                    break;
                }

                if ($this->skipSync()) {
                    if ($this->isEof()) {
                        break;
                    }
                }

                $this->readBlockHeader();
            }
            $data[] = $this->datumReader->read($this->decoder);

            $this->blockCount -= 1;
        }

        return $data;
    }

    /**
     * Closes this writer (and its AvroIO object.)
     *
     * @uses IOInterface::close()
     *
     * @throws AvroNotImplementedException
     */
    public function close()
    {
        return $this->io->close();
    }
}
