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
 * Class AvroArraySchema
 *
 * Avro array schema, consisting of items of a particular Avro schema type.
 */
class AvroArraySchema extends AvroSchema implements AvroArraySchemaInterface
{
    /** @var AvroNameInterface|AvroSchemaInterface named schema name or AvroSchema of array element */
    private $items;
    /**
     * @var boolean true if the items schema
     * FIXME: couldn't we derive this from whether or not $this->items is an AvroName or an Avro\Schema\AvroSchema?
     */
    private $isItemsSchemaFromSchemata;

    /**
     * @param string|AvroNamedSchemaInterface $items            AvroNamedSchema name or object form
     *                                                          of decoded JSON schema representation.
     * @param string                          $defaultNamespace Namespace of enclosing schema
     * @param AvroNamedSchemataInterface      &$schemata
     *
     * @throws AvroSchemaParseException
     */
    public function __construct($items, $defaultNamespace, &$schemata = null)
    {
        parent::__construct(AvroSchema::ARRAY_SCHEMA);

        $this->isItemsSchemaFromSchemata = false;
        $itemsSchema                     = null;
        if (is_string($items)
            && $itemsSchema = $schemata->getSchemaByName(new AvroName($items, null, $defaultNamespace))) {
            $this->isItemsSchemaFromSchemata = true;
        } else {
            $itemsSchema = AvroSchema::subparse($items, $defaultNamespace, $schemata);
        }

        $this->items = $itemsSchema;
    }


    /**
     * @return AvroNameInterface|AvroSchemaInterface named schema name or Avro\Schema\AvroSchema of this array schema's elements.
     */
    public function getItems()
    {
        return $this->items;
    }

    /**
     * @return mixed
     */
    public function toAvro()
    {
        $avro = parent::toAvro();

        $avro[AvroSchema::ITEMS_ATTR] = $this->isItemsSchemaFromSchemata
            ? $this->items->getQualifiedName()
            : $this->items->toAvro();

        return $avro;
    }
}
