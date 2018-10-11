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

namespace Avro\Datum;

use Avro\Exception\AvroException;
use Avro\Exception\Datum\AvroIOTypeException;
use Avro\Exception\Schema\AvroSchemaParseException;
use Avro\Schema\AvroArraySchemaInterface;
use Avro\Schema\AvroEnumSchemaInterface;
use Avro\Schema\AvroFixedSchemaInterface;
use Avro\Schema\AvroMapSchemaInterface;
use Avro\Schema\AvroRecordSchemaInterface;
use Avro\Schema\AvroSchema;
use Avro\Schema\AvroSchemaInterface;
use Avro\Schema\AvroUnionSchemaInterface;

/**
 * Class AvroIODatumWriter
 *
 * Handles schema-specific writing of Data to the encoder.
 * Ensures that each datum written is consistent with the writer's schema.
 */
class AvroIODatumWriter implements IODatumWriterInterface
{
    /** @var AvroSchemaInterface Schema used by this instance to write Avro Data. */
    private $writersSchema;

    /**
     * AvroIODatumWriter constructor.
     *
     * @param AvroSchemaInterface|null $writersSchema
     */
    public function __construct(AvroSchemaInterface $writersSchema = null)
    {
        $this->writersSchema = $writersSchema;
    }

    /**#@+
     * @param AvroArraySchemaInterface|AvroSchemaInterface $writersSchema
     * @param null|boolean|int|float|string|array          $datum item to be written
     * @param IOBinaryEncoderInterface                     $encoder
     *
     * @throws AvroException
     * @throws AvroIOTypeException
     * @throws AvroSchemaParseException
     */
    private function writeArray(AvroArraySchemaInterface $writersSchema, $datum, IOBinaryEncoderInterface $encoder)
    {
        $datumCount = count($datum);
        if (0 < $datumCount) {
            $encoder->writeLong($datumCount);
            $items = $writersSchema->getItems();
            foreach ($datum as $item) {
                $this->writeData($items, $item, $encoder);
            }
        }

        return $encoder->writeLong(0);
    }

    /**
     * @param AvroMapSchemaInterface|AvroSchemaInterface $writersSchema
     * @param null|boolean|int|float|string|array        $datum
     * @param IOBinaryEncoderInterface                   $encoder
     *
     * @throws AvroException
     * @throws AvroIOTypeException
     */
    private function writeMap(AvroMapSchemaInterface $writersSchema, $datum, IOBinaryEncoderInterface $encoder)
    {
        $datum_count = count($datum);
        if ($datum_count > 0) {
            $encoder->writeLong($datum_count);
            foreach ($datum as $k => $v) {
                $encoder->writeString($k);
                $this->writeData($writersSchema->getValues(), $v, $encoder);
            }
        }
        $encoder->writeLong(0);
    }

    /**
     * @param AvroUnionSchemaInterface|AvroSchemaInterface $writersSchema
     * @param null|boolean|int|float|string|array          $datum
     * @param IOBinaryEncoderInterface                     $encoder
     *
     * @throws AvroException
     * @throws AvroIOTypeException
     * @throws AvroSchemaParseException
     */
    private function writeUnion(AvroUnionSchemaInterface $writersSchema, $datum, IOBinaryEncoderInterface $encoder)
    {
        $datum_schema_index = -1;
        $datum_schema       = null;
        foreach ($writersSchema->getSchemas() as $index => $schema) {
            if (AvroSchema::isValidDatum($schema, $datum)) {
                $datum_schema_index = $index;
                $datum_schema       = $schema;
                break;
            }
        }

        if (is_null($datum_schema)) {
            throw new AvroIOTypeException($writersSchema, $datum);
        }

        $encoder->writeLong($datum_schema_index);
        $this->writeData($datum_schema, $datum, $encoder);
    }

    /**
     * @param AvroEnumSchemaInterface|AvroSchemaInterface $writersSchema
     * @param null|boolean|int|float|string|array         $datum
     * @param IOBinaryEncoderInterface                    $encoder
     *
     * @return mixed
     */
    private function writeEnum(AvroEnumSchemaInterface $writersSchema, $datum, IOBinaryEncoderInterface $encoder)
    {
        $datum_index = $writersSchema->getSymbolIndex($datum);

        return $encoder->writeInt($datum_index);
    }

    /**
     * @param AvroFixedSchemaInterface|AvroSchemaInterface $writersSchema
     * @param null|boolean|int|float|string|array          $datum
     * @param IOBinaryEncoderInterface                     $encoder
     *
     * @return mixed
     */
    private function writeFixed(AvroFixedSchemaInterface $writersSchema, $datum, IOBinaryEncoderInterface $encoder)
    {
        /**
         * NOTE Unused $writers_schema parameter included for consistency
         * with other write_* methods.
         */
        return $encoder->write($datum);
    }

    /**
     * @param AvroRecordSchemaInterface|AvroSchemaInterface $writersSchema
     * @param null|boolean|int|float|string|array           $datum
     * @param IOBinaryEncoderInterface                      $encoder
     *
     * @throws AvroException
     * @throws AvroIOTypeException
     */
    private function writeRecord(AvroRecordSchemaInterface $writersSchema, $datum, IOBinaryEncoderInterface $encoder)
    {
        foreach ($writersSchema->getFields() as $field) {
            $this->writeData($field->getType(), $datum[$field->getName()], $encoder);
        }
    }

    /**
     * @param AvroSchemaInterface $writersSchema
     *
     * @return AvroIODatumWriter
     */
    public function setWritersSchema(AvroSchemaInterface $writersSchema)
    {
        $this->writersSchema = $writersSchema;

        return $this;
    }

    /**
     * @param AvroSchemaInterface|string $writersSchema
     * @param                            $datum
     * @param IOBinaryEncoderInterface   $encoder
     *
     * @return mixed
     * @throws AvroException
     * @throws AvroIOTypeException if $datum is invalid for $writers_schema
     * @throws AvroSchemaParseException
     */
    public function writeData(AvroSchemaInterface $writersSchema, $datum, IOBinaryEncoderInterface $encoder)
    {
        if (!AvroSchema::isValidDatum($writersSchema, $datum)) {
            throw new AvroIOTypeException($writersSchema, $datum);
        }

        switch ($writersSchema->getType()) {
            case AvroSchema::NULL_TYPE:
                return $encoder->writeNull($datum);
            case AvroSchema::BOOLEAN_TYPE:
                return $encoder->writeBoolean($datum);
            case AvroSchema::INT_TYPE:
                return $encoder->writeInt($datum);
            case AvroSchema::LONG_TYPE:
                return $encoder->writeLong($datum);
            case AvroSchema::FLOAT_TYPE:
                return $encoder->writeFloat($datum);
            case AvroSchema::DOUBLE_TYPE:
                return $encoder->writeDouble($datum);
            case AvroSchema::STRING_TYPE:
                return $encoder->writeString($datum);
            case AvroSchema::BYTES_TYPE:
                return $encoder->writeBytes($datum);
            case AvroSchema::ARRAY_SCHEMA:
                return $this->writeArray($writersSchema, $datum, $encoder);
            case AvroSchema::MAP_SCHEMA:
                $this->writeMap($writersSchema, $datum, $encoder);
                break;
            case AvroSchema::FIXED_SCHEMA:
                return $this->writeFixed($writersSchema, $datum, $encoder);
            case AvroSchema::ENUM_SCHEMA:
                return $this->writeEnum($writersSchema, $datum, $encoder);
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
            case AvroSchema::REQUEST_SCHEMA:
                $this->writeRecord($writersSchema, $datum, $encoder);
                break;
            case AvroSchema::UNION_SCHEMA:
                $this->writeUnion($writersSchema, $datum, $encoder);
                break;
            default:
                throw new AvroException(sprintf('Uknown type: %s', $writersSchema->getType()));
        }
    }

    /**
     * @param                          $datum
     * @param IOBinaryEncoderInterface $encoder
     *
     * @throws AvroException
     * @throws AvroIOTypeException
     * @throws AvroSchemaParseException
     */
    public function write($datum, IOBinaryEncoderInterface $encoder)
    {
        $this->writeData($this->writersSchema, $datum, $encoder);
    }
}
