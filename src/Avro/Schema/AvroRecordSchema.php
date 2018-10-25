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
use Avro\Util\AvroUtil;

/**
 * Class AvroRecordSchema
 */
class AvroRecordSchema extends AvroNamedSchema implements AvroRecordSchemaInterface
{
    /** @var AvroNamedSchemaInterface[] Array of AvroNamedSchema field definitions of this AvroRecordSchema */
    private $fields;

    /**
     * @var array Map of field names to field objects.
     * @internal Not called directly. Memoization of AvroRecordSchema->fieldsHash()
     */
    private $fieldsHash;

    /**
     * AvroRecordSchema constructor.
     *
     * @param AvroNameInterface          $name
     * @param string                     $doc
     * @param array                      $fields
     * @param AvroNamedSchemataInterface &$schemata
     * @param string                     $schemaType schema type name
     *
     * @throws AvroSchemaParseException
     * @internal param string $namespace
     */
    public function __construct($name, $doc, $fields, &$schemata = null, $schemaType = AvroSchema::RECORD_SCHEMA)
    {
        if (is_null($fields)) {
            throw new AvroSchemaParseException('Record schema requires a non-empty fields attribute');
        }

        if (AvroSchema::REQUEST_SCHEMA == $schemaType) {
            parent::__construct($schemaType, $name);
        } else {
            parent::__construct($schemaType, $name, $doc, $schemata);
        }

        list(, $namespace) = $name->getNameAndNamespace();
        $this->fields = self::parseFields($fields, $namespace, $schemata);
    }

    /**
     * @param mixed                      $fieldData
     * @param string                     $defaultNamespace Namespace of enclosing schema
     * @param AvroNamedSchemataInterface &$schemata
     *
     * @return AvroFieldInterface[]
     * @throws AvroSchemaParseException
     */
    public static function parseFields($fieldData, $defaultNamespace, &$schemata)
    {
        $fields     = [];
        $fieldNames = [];

        foreach ($fieldData as $index => $field) {
            $name  = AvroUtil::arrayValue($field, AvroField::FIELD_NAME_ATTR);
            $type  = AvroUtil::arrayValue($field, AvroSchema::TYPE_ATTR);
            $order = AvroUtil::arrayValue($field, AvroField::ORDER_ATTR);
            $doc   = AvroUtil::arrayValue($field, AvroSchema::DOC_ATTR);

            $default    = null;
            $hasDefault = false;
            if (array_key_exists(AvroField::DEFAULT_ATTR, $field)) {
                $default    = $field[AvroField::DEFAULT_ATTR];
                $hasDefault = true;
            }

            if (in_array($name, $fieldNames)) {
                throw new AvroSchemaParseException(sprintf("Field name %s is already in use", $name));
            }

            $isSchemaFromSchemata = false;
            $fieldSchema          = null;
            if (is_string($type)
                && $fieldSchema = $schemata->getSchemaByName(new AvroName($type, null, $defaultNamespace))) {
                $isSchemaFromSchemata = true;
            } else {
                $fieldSchema = self::subparse($type, $defaultNamespace, $schemata);
            }

            $newField = new AvroField(
                $name,
                $fieldSchema,
                $isSchemaFromSchemata,
                $hasDefault,
                $default,
                $order,
                $doc
            );

            $fieldNames [] = $name;
            $fields []     = $newField;
        }

        return $fields;
    }

    /**
     * @return mixed
     */
    public function toAvro()
    {
        $avro = parent::toAvro();

        $fieldsAvro = [];
        foreach ($this->fields as $field) {
            $fieldsAvro [] = $field->toAvro();
        }

        if (AvroSchema::REQUEST_SCHEMA == $this->type) {
            return $fieldsAvro;
        }

        $avro[AvroSchema::FIELDS_ATTR] = $fieldsAvro;

        return $avro;
    }

    /**
     * @return array The schema definitions of the fields of this AvroRecordSchema
     */
    public function getFields()
    {
        return $this->fields;
    }

    /**
     * @return array Hash table of the fields of this AvroRecordSchema fields keyed by each field's name
     */
    public function getFieldsHash()
    {
        if (is_null($this->fieldsHash)) {
            $hash = [];
            foreach ($this->fields as $field) {
                $hash[$field->getName()] = $field;
            }
            $this->fieldsHash = $hash;
        }

        return $this->fieldsHash;
    }
}
