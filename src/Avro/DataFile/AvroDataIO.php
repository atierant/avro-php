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

use Avro\Datum\AvroIODatumReader;
use Avro\Datum\AvroIODatumWriter;
use Avro\Exception\AvroException;
use Avro\Exception\AvroNotImplementedException;
use Avro\Exception\Data\AvroDataIOException;
use Avro\Exception\Datum\AvroIOSchemaMatchException;
use Avro\Exception\Datum\AvroIOTypeException;
use Avro\Exception\IO\AvroIOException;
use Avro\Exception\Schema\AvroSchemaParseException;
use Avro\IO\AvroFile;
use Avro\IO\AbstractAvroIO;
use Avro\Schema\AvroSchema;
use Avro\Schema\AvroSchemaInterface;

/**
 * Class AvroDataIO
 */
class AvroDataIO
{
    /** @var int used in file header */
    const VERSION = 1;

    /** @var int count of bytes in synchronization marker */
    const SYNC_SIZE = 16;

    /**
     * @var int Count of items per block, arbitrarily set to 4000 * SYNC_SIZE
     * @todo make this value configurable
     */
    const SYNC_INTERVAL = 64000;

    /** @var string map key for datafile metadata codec value */
    const METADATA_CODEC_ATTR = 'avro.codec';

    /** @var string map key for datafile metadata schema value */
    const METADATA_SCHEMA_ATTR = 'avro.schema';

    /** @var string JSON for datafile metadata schema */
    const METADATA_SCHEMA_JSON = '{"type":"map","values":"bytes"}';

    /** @var string codec value for NULL codec */
    const NULL_CODEC = 'null';

    /** @var string codec value for deflate codec */
    const DEFLATE_CODEC = 'deflate';

    /**
     * @var array array of valid codec names
     * @todo Avro implementations are required to implement deflate codec as well, so implement it already!
     */
    private static $validCodecs = [self::NULL_CODEC];

    /** @var AvroSchemaInterface cached version of metadata schema object */
    private static $metadataSchema;

    /**
     * @return array array of valid codecs
     */
    private static function validCodecs()
    {
        return self::$validCodecs;
    }

    /**
     * @param AbstractAvroIO      $io
     * @param AvroSchemaInterface $schema
     *
     * @return DataIOWriterInterface
     * @throws AvroDataIOException
     * @throws AvroException
     * @throws AvroNotImplementedException
     * @throws AvroIOTypeException
     * @throws AvroIOException
     * @throws AvroSchemaParseException
     */
    protected static function openWriter($io, $schema)
    {
        $writer = new AvroIODatumWriter($schema);

        return new AvroDataIOWriter($io, $writer, $schema);
    }

    /**
     * @param AbstractAvroIO      $io
     * @param AvroSchemaInterface $schema
     *
     * @return DataIOReaderInterface
     * @throws AvroDataIOException
     * @throws AvroException
     * @throws AvroNotImplementedException
     * @throws AvroSchemaParseException
     * @throws AvroIOSchemaMatchException
     */
    protected static function openReader($io, $schema)
    {
        $reader = new AvroIODatumReader(null, $schema);

        return new AvroDataIOReader($io, $reader);
    }

    /**
     * @return string The initial "magic" segment of an Avro container file header.
     */
    public static function magic()
    {
        return ('Obj' . pack('c', self::VERSION));
    }

    /**
     * @return int Count of bytes in the initial "magic" segment of the Avro container file header
     */
    public static function magicSize()
    {
        return strlen(self::magic());
    }

    /**
     * @return AvroSchemaInterface object of Avro container file metadata.
     * @throws AvroSchemaParseException
     */
    public static function metadatSchema()
    {
        if (is_null(self::$metadataSchema)) {
            self::$metadataSchema = AvroSchema::parse(self::METADATA_SCHEMA_JSON);
        }

        return self::$metadataSchema;
    }

    /**
     * @param string $filePath   file_path of file to open
     * @param string $mode       one of AvroFile::READ_MODE or Avro\IO\AvroFile::WRITE_MODE
     * @param string $schemaJson JSON of writer's schema
     *
     * @return DataIOReaderInterface|DataIOWriterInterface
     *
     * @throws AvroDataIOException if $writers_schema is not provided
     * @throws AvroIOException or if an invalid $mode is given.
     * @throws AvroIOTypeException
     * @throws AvroNotImplementedException
     * @throws AvroSchemaParseException
     * @throws AvroException
     */
    public static function openFile($filePath, $mode = AvroFile::READ_MODE, $schemaJson = null)
    {
        $schema = !is_null($schemaJson) ? AvroSchema::parse($schemaJson) : null;

        switch ($mode) {
            case AvroFile::WRITE_MODE:
                if (is_null($schema)) {
                    throw new AvroDataIOException('Writing an Avro file requires a schema.');
                }
                $file = new AvroFile($filePath, AvroFile::WRITE_MODE);
                $io   = self::openWriter($file, $schema);
                break;
            case AvroFile::READ_MODE:
                $file = new AvroFile($filePath, AvroFile::READ_MODE);
                $io   = self::openReader($file, $schema);
                break;
            default:
                throw new AvroDataIOException(sprintf(
                    "Only modes '%s' and '%s' allowed. You gave '%s'.",
                    AvroFile::READ_MODE,
                    AvroFile::WRITE_MODE,
                    $mode
                ));
        }

        return $io;
    }

    /**
     * @param string $codec
     *
     * @return boolean true if $codec is a valid codec value and false otherwise
     */
    public static function isValidCodec($codec)
    {
        return in_array($codec, self::validCodecs());
    }
}