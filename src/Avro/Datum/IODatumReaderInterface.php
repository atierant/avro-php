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
use Avro\Exception\AvroNotImplementedException;
use Avro\Exception\Datum\AvroIOSchemaMatchException;
use Avro\Schema\AvroArraySchemaInterface;
use Avro\Schema\AvroEnumSchemaInterface;
use Avro\Schema\AvroMapSchemaInterface;
use Avro\Schema\AvroSchemaInterface;
use Avro\Schema\AvroUnionSchemaInterface;

/**
 * Interface IODatumReaderInterface
 *
 * Handles schema-specifc reading of Data from the decoder.
 * Also handles schema resolution between the reader and writer schemas (if a writer's schema is provided).
 */
interface IODatumReaderInterface
{
    /**
     * @param AvroSchemaInterface $readersSchema
     */
    public function setWritersSchema(AvroSchemaInterface $readersSchema);

    /**
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return mixed
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function read(IOBinaryDecoderInterface $decoder);

    /**
     * @param AvroSchemaInterface      $writersSchema
     * @param AvroSchemaInterface      $readersSchema
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return mixed
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function readData($writersSchema, $readersSchema, $decoder);

    /**
     * @param AvroArraySchemaInterface $writersSchema
     * @param AvroArraySchemaInterface $readersSchema
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return array
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function readArray($writersSchema, $readersSchema, $decoder);

    /**
     * @param AvroMapSchemaInterface   $writersSchema
     * @param AvroMapSchemaInterface   $readersSchema
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return array
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function readMap($writersSchema, $readersSchema, $decoder);

    /**
     * @param AvroUnionSchemaInterface $writersSchema
     * @param AvroUnionSchemaInterface $readersSchema
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return mixed
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function readUnion($writersSchema, $readersSchema, $decoder);

    /**
     * @param AvroEnumSchemaInterface  $writersSchema
     * @param AvroEnumSchemaInterface  $readersSchema
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return string
     * @throws AvroNotImplementedException
     */
    public function readEnum($writersSchema, $readersSchema, $decoder);

    /**
     * @param AvroSchemaInterface      $writersSchema
     * @param AvroSchemaInterface      $readersSchema
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return string
     * @throws AvroNotImplementedException
     */
    public function readFixed($writersSchema, $readersSchema, $decoder);

    /**
     * @param AvroSchemaInterface      $writersSchema
     * @param AvroSchemaInterface      $readersSchema
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return array
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function readRecord($writersSchema, $readersSchema, $decoder);

    /**
     * @param AvroSchemaInterface                 $fieldSchema
     * @param null|boolean|int|float|string|array $defaultValue
     *
     * @return null|boolean|int|float|string|array
     *
     * @throws AvroException if $field_schema type is unknown.
     */
    public function readDefaultValue($fieldSchema, $defaultValue);
}
