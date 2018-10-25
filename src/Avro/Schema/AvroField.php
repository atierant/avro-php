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
 * Class AvroField
 *
 * Field of an {@link AvroRecordSchema}
 */
class AvroField extends AvroSchema implements AvroFieldInterface
{
    /** @var string fields name attribute name */
    const FIELD_NAME_ATTR = 'name';
    /** @var string */
    const DEFAULT_ATTR = 'default';
    /** @var string */
    const ORDER_ATTR = 'order';
    /** @var string */
    const ASC_SORT_ORDER = 'ascending';
    /** @var string */
    const DESC_SORT_ORDER = 'descending';
    /** @var string */
    const IGNORE_SORT_ORDER = 'ignore';
    /** @var array list of valid field sort order values */
    private static $validFieldSortOrders = [self::ASC_SORT_ORDER, self::DESC_SORT_ORDER, self::IGNORE_SORT_ORDER];
    /** @var string */
    private $name;
    /** @var boolean whether or no there is a default value */
    private $hasDefault;
    /** @var string field default value */
    private $default;
    /** @var string sort order of this field */
    private $order;
    /** @var boolean whether or not the AvroNamedSchema of this field is defined in the AvroNamedSchemata instance */
    private $isTypeFromSchemata;
    /** * @var string documentation of this field */
    private $doc;

    /**
     * @param string                  $name
     * @param AvroSchemaInterface     $schema
     * @param boolean                 $isTypeFromSchemata
     * @param                         $hasDefault
     * @param string                  $default
     * @param string                  $order
     * @param null                    $doc
     *
     * @throws AvroSchemaParseException
     * @internal param string $type
     * @todo     Check validity of $default value
     * @todo     Check validity of $order value
     */
    public function __construct($name, $schema, $isTypeFromSchemata, $hasDefault, $default, $order = null, $doc = null)
    {
        if (!AvroName::isWellFormedName($name)) {
            throw new AvroSchemaParseException('Field requires a "name" attribute');
        }

        $this->type               = $schema;
        $this->isTypeFromSchemata = $isTypeFromSchemata;
        $this->name               = $name;
        $this->hasDefault         = $hasDefault;
        if ($this->hasDefault) {
            $this->default = $default;
        }
        self::checkOrderValue($order);
        $this->order = $order;
        $this->doc   = $doc;
    }

    /**
     * @param string $order
     *
     * @return boolean
     */
    private static function isValidFieldSortOrder($order)
    {
        return in_array($order, self::$validFieldSortOrders);
    }

    /**
     * @param string $order
     *
     * @throws AvroSchemaParseException if $order is not a valid field order value.
     */
    private static function checkOrderValue($order)
    {
        if (!is_null($order) && !self::isValidFieldSortOrder($order)) {
            throw new AvroSchemaParseException(sprintf('Invalid field sort order %s', $order));
        }
    }

    /**
     * @return mixed
     */
    public function toAvro()
    {
        $avro = [AvroField::FIELD_NAME_ATTR => $this->name];

        $avro[AvroSchema::TYPE_ATTR] = $this->isTypeFromSchemata
            ? $this->getType()->getQualifiedName()
            : $this->getType()->toAvro();

        if ($this->hasDefault) {
            $avro[AvroField::DEFAULT_ATTR] = $this->default;
        }

        if ($this->order) {
            $avro[AvroField::ORDER_ATTR] = $this->order;
        }

        if ($this->doc) {
            $avro[AvroSchema::DOC_ATTR] = $this->doc;
        }

        return $avro;
    }

    /**
     * @return string the name of this field
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * @return mixed the default value of this field
     */
    public function getDefaultValue()
    {
        return $this->default;
    }

    /**
     * @return boolean true if the field has a default and false otherwise
     */
    public function hasDefaultValue()
    {
        return $this->hasDefault;
    }

    /**
     * @return string the documentation of this field
     */
    public function getDoc()
    {
        return $this->doc;
    }
}
