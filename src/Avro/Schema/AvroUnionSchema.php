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
 * Class AvroUnionSchema
 *
 * Union of Avro schemas, of which values can be of any of the schema in the union.
 */
class AvroUnionSchema extends AvroSchema implements AvroUnionSchemaInterface
{
    /** @var int[] list of indices of named schemas which are defined in $schemata */
    public $schemaFromSchemataIndices;
    /** @var AvroSchemaInterface[]|AvroNamedSchemaInterface[] list of schemas of this union */
    private $schemas;

    /**
     * @param AvroSchemaInterface[] $schemas          list of schemas in the union
     * @param string                $defaultNamespace namespace of enclosing schema
     * @param AvroNamedSchemata     &$schemata
     *
     * @throws AvroSchemaParseException
     */
    public function __construct($schemas, $defaultNamespace, &$schemata = null)
    {
        parent::__construct(AvroSchema::UNION_SCHEMA);

        $this->schemaFromSchemataIndices = [];
        $schemaTypes                    = [];
        foreach ($schemas as $index => $schema) {
            $isSchemaFromSchemata = false;
            $newSchema            = null;
            if (is_string($schema)
                && ($newSchema = $schemata->getSchemaByName(new AvroName($schema, null, $defaultNamespace)))) {
                $isSchemaFromSchemata = true;
            } else {
                $newSchema = self::subparse($schema, $defaultNamespace, $schemata);
            }

            $schemaType = $newSchema->getType();
            if (self::isValidType($schemaType)
                && !self::isNamedType($schemaType)
                && in_array($schemaType, $schemaTypes)) {
                throw new AvroSchemaParseException(sprintf('"%s" is already in union', $schemaType));
            } elseif (AvroSchema::UNION_SCHEMA == $schemaType) {
                throw new AvroSchemaParseException('Unions cannot contain other unions');
            } else {
                $schemaTypes []  = $schemaType;
                $this->schemas [] = $newSchema;
                if ($isSchemaFromSchemata) {
                    $this->schemaFromSchemataIndices [] = $index;
                }
            }
        }
    }

    /**
     * @return AvroNamedSchemaInterface[]|AvroSchemaInterface[]
     */
    public function getSchemas()
    {
        return $this->schemas;
    }

    /**
     * @param $index
     *
     * @return AvroSchemaInterface the particular schema from the union for
     * the given (zero-based) index.
     * @throws AvroSchemaParseException if the index is invalid for this schema.
     */
    public function getSchemaByIndex($index)
    {
        if (count($this->schemas) > $index) {
            return $this->schemas[$index];
        }

        throw new AvroSchemaParseException('Invalid union schema index');
    }

    /**
     * @return array|mixed
     */
    public function toAvro()
    {
        $avro = [];

        foreach ($this->schemas as $index => $schema) {
            $avro [] = in_array($index, $this->schemaFromSchemataIndices)
                ? $schema->getQualifiedName()
                : $schema->toAvro();
        }

        return $avro;
    }
}
