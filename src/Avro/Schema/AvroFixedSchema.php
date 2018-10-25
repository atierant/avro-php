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

namespace Avro\Schema;

use Avro\Exception\Schema\AvroSchemaParseException;

/**
 * Class AvroFixedSchema
 *
 * AvroNamedSchema with fixed-length Data values
 */
class AvroFixedSchema extends AvroNamedSchema implements AvroFixedSchemaInterface
{
    /** @var int byte count of this fixed schema Data value */
    private $size;

    /**
     * @param AvroNameInterface          $name
     * @param string                     $doc  Set to null, as fixed schemas don't have doc strings
     * @param int                        $size byte count of this fixed schema Data value
     * @param AvroNamedSchemataInterface &$schemata
     *
     * @throws AvroSchemaParseException
     */
    public function __construct($name, $doc, $size, &$schemata = null)
    {
        unset($doc);
        $doc = null; // Fixed schemas don't have doc strings.
        if (!is_integer($size)) {
            throw new AvroSchemaParseException('Fixed Schema requires a valid integer for "size" attribute');
        }
        parent::__construct(AvroSchema::FIXED_SCHEMA, $name, $doc, $schemata);

        return $this->size = $size;
    }

    /**
     * @return int byte count of this fixed schema Data value
     */
    public function getSize()
    {
        return $this->size;
    }

    /**
     * @return mixed
     */
    public function toAvro()
    {
        $avro = parent::toAvro();

        $avro[AvroSchema::SIZE_ATTR] = $this->size;

        return $avro;
    }
}
