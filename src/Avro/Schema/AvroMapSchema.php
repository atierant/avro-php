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
 * Class AvroMapSchema
 *
 * Avro map schema consisting of named values of defined Avro Schema types.
 */
class AvroMapSchema extends AvroSchema implements AvroMapSchemaInterface
{
    /** @var string|AvroSchemaInterface|AvroNamedSchemaInterface named schema name or AvroSchema of map schema values. */
    private $values;

    /**
     * @var boolean true if the named schema XXX
     * Couldn't we derive this based on whether or not $this->values is a string?
     */
    private $isValuesSchemaFromSchemata;

    /**
     * @param string|AvroSchemaInterface $values
     * @param string                     $defaultNamespace namespace of enclosing schema
     * @param AvroNamedSchemataInterface &$schemata
     *
     * @throws AvroSchemaParseException
     */
    public function __construct($values, $defaultNamespace, &$schemata = null)
    {
        parent::__construct(AvroSchema::MAP_SCHEMA);

        $this->isValuesSchemaFromSchemata = false;
        $valuesSchema                     = null;
        if (is_string($values)
            && $valuesSchema = $schemata->getSchemaByName(new AvroName($values, null, $defaultNamespace))) {
            $this->isValuesSchemaFromSchemata = true;
        } else {
            $valuesSchema = AvroSchema::subparse($values, $defaultNamespace, $schemata);
        }

        $this->values = $valuesSchema;
    }

    /**
     * @return AvroSchemaInterface
     */
    public function getValues()
    {
        return $this->values;
    }

    /**
     * @return mixed
     */
    public function toAvro()
    {
        $avro = parent::toAvro();

        $avro[AvroSchema::VALUES_ATTR] = $this->isValuesSchemaFromSchemata
            ? $this->values->getQualifiedName()
            : $this->values->toAvro();

        return $avro;
    }
}
