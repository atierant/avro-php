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
use Avro\Exception\Datum\AvroIOSchemaMatchException;
use Avro\Schema\AvroArraySchemaInterface;
use Avro\Schema\AvroEnumSchemaInterface;
use Avro\Schema\AvroFixedSchemaInterface;
use Avro\Schema\AvroMapSchemaInterface;
use Avro\Schema\AvroRecordSchemaInterface;
use Avro\Schema\AvroSchema;
use Avro\Schema\AvroSchemaInterface;
use Avro\Schema\AvroUnionSchemaInterface;

/**
 * Class AvroIODatumReader
 *
 * Handles schema-specifc reading of Data from the decoder.
 * Also handles schema resolution between the reader and writer schemas (if a writer's schema is provided).
 */
class AvroIODatumReader implements IODatumReaderInterface
{
    /** @var AvroSchemaInterface */
    private $writersSchema;
    /** @var AvroSchemaInterface */
    private $readersSchema;

    /**
     * AvroIODatumReader constructor.
     *
     * @param AvroSchemaInterface|null $writersSchema
     * @param AvroSchemaInterface|null $readersSchema
     */
    public function __construct(AvroSchemaInterface $writersSchema = null, AvroSchemaInterface $readersSchema = null)
    {
        $this->writersSchema = $writersSchema;
        $this->readersSchema = $readersSchema;
    }

    /**
     * @param AvroSchemaInterface|string $writersSchema
     * @param IOBinaryDecoderInterface   $decoder
     *
     * @return mixed
     * @throws AvroException
     */
    private function skipData(AvroSchemaInterface $writersSchema, IOBinaryDecoderInterface $decoder)
    {
        switch ($writersSchema->getType()) {
            case AvroSchema::NULL_TYPE:
                return $decoder->skipNull();
            case AvroSchema::BOOLEAN_TYPE:
                return $decoder->skipBoolean();
            case AvroSchema::INT_TYPE:
                return $decoder->skipInt();
            case AvroSchema::LONG_TYPE:
                return $decoder->skipInt();
            case AvroSchema::FLOAT_TYPE:
                return $decoder->skipFloat();
            case AvroSchema::DOUBLE_TYPE:
                return $decoder->skipDouble();
            case AvroSchema::STRING_TYPE:
                return $decoder->skipString();
            case AvroSchema::BYTES_TYPE:
                return $decoder->skipBytes();
            case AvroSchema::ARRAY_SCHEMA:
                return $decoder->skip_array($writersSchema, $decoder);
            case AvroSchema::MAP_SCHEMA:
                return $decoder->skip_map($writersSchema, $decoder);
            case AvroSchema::UNION_SCHEMA:
                return $decoder->skip_union($writersSchema, $decoder);
            case AvroSchema::ENUM_SCHEMA:
                return $decoder->skip_enum($writersSchema, $decoder);
            case AvroSchema::FIXED_SCHEMA:
                return $decoder->skip_fixed($writersSchema, $decoder);
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
            case AvroSchema::REQUEST_SCHEMA:
                return $decoder->skip_record($writersSchema, $decoder);
            default:
                throw new AvroException(sprintf('Uknown schema type: %s', $writersSchema->getType()));
        }
    }

    /**
     * Checks equivalence of the given attributes of the two given schemas.
     *
     * @param AvroSchemaInterface $schemaOne
     * @param AvroSchemaInterface $schemaTwo
     * @param string[]            $attributeNames array of string attribute names to compare
     *
     * @return boolean true if the attributes match and false otherwise.
     */
    protected static function attributesMatch($schemaOne, $schemaTwo, $attributeNames)
    {
        foreach ($attributeNames as $attributeName) {
            if ($schemaOne->attribute($attributeName) != $schemaTwo->attribute($attributeName)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param AvroSchemaInterface|AvroMapSchemaInterface|AvroEnumSchemaInterface|AvroArraySchemaInterface $writersSchema
     * @param AvroSchemaInterface|AvroMapSchemaInterface|AvroEnumSchemaInterface|AvroArraySchemaInterface $readersSchema
     *
     * @return bool true if the attributes match each other and false otherwise.
     */
    public static function schemasMatch(AvroSchemaInterface $writersSchema, AvroSchemaInterface $readersSchema)
    {
        $writersSchemaType = $writersSchema->getType();
        $readersSchemaType = $readersSchema->getType();

        if (AvroSchema::UNION_SCHEMA == $writersSchemaType
            || AvroSchema::UNION_SCHEMA == $readersSchemaType) {
            return true;
        }

        if ($writersSchemaType == $readersSchemaType) {
            if (AvroSchema::isPrimitiveType($writersSchemaType)) {
                return true;
            }

            switch ($readersSchemaType) {
                case AvroSchema::MAP_SCHEMA:
                    return self::attributesMatch(
                        $writersSchema->getValues(),
                        $readersSchema->getValues(),
                        [AvroSchema::TYPE_ATTR]
                    );
                case AvroSchema::ARRAY_SCHEMA:
                    return self::attributesMatch(
                        $writersSchema->getItems(),
                        $readersSchema->getItems(),
                        [AvroSchema::TYPE_ATTR]
                    );
                case AvroSchema::ENUM_SCHEMA:
                    return self::attributesMatch(
                        $writersSchema,
                        $readersSchema,
                        [AvroSchema::FULLNAME_ATTR]
                    );
                case AvroSchema::FIXED_SCHEMA:
                    return self::attributesMatch(
                        $writersSchema,
                        $readersSchema,
                        [AvroSchema::FULLNAME_ATTR, AvroSchema::SIZE_ATTR]
                    );
                case AvroSchema::RECORD_SCHEMA:
                case AvroSchema::ERROR_SCHEMA:
                    return self::attributesMatch($writersSchema, $readersSchema, [AvroSchema::FULLNAME_ATTR]);
                case AvroSchema::REQUEST_SCHEMA:
                    // XXX: This seems wrong
                    return true;
                // XXX: no default
            }

            if (AvroSchema::INT_TYPE == $writersSchemaType
                && in_array($readersSchemaType, [AvroSchema::LONG_TYPE,
                    AvroSchema::FLOAT_TYPE,
                    AvroSchema::DOUBLE_TYPE])) {
                return true;
            }

            if (AvroSchema::LONG_TYPE == $writersSchemaType
                && in_array($readersSchemaType, [AvroSchema::FLOAT_TYPE,
                    AvroSchema::DOUBLE_TYPE])) {
                return true;
            }

            if (AvroSchema::FLOAT_TYPE == $writersSchemaType
                && AvroSchema::DOUBLE_TYPE == $readersSchemaType) {
                return true;
            }

            return false;
        }

        return false;
    }

    /**
     * @param AvroSchemaInterface $readersSchema
     */
    public function setWritersSchema(AvroSchemaInterface $readersSchema)
    {
        $this->writersSchema = $readersSchema;
    }

    /**
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return mixed
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function read(IOBinaryDecoderInterface $decoder)
    {
        if (is_null($this->readersSchema)) {
            $this->readersSchema = $this->writersSchema;
        }

        return $this->readData($this->writersSchema, $this->readersSchema, $decoder);
    }

    /**
     * @param AvroSchemaInterface|AvroUnionSchemaInterface|string $writersSchema
     * @param AvroSchemaInterface|AvroUnionSchemaInterface|string $readersSchema
     * @param IOBinaryDecoderInterface                            $decoder
     *
     * @return mixed
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function readData($writersSchema, $readersSchema, $decoder)
    {
        if (!self::schemasMatch($writersSchema, $readersSchema)) {
            throw new AvroIOSchemaMatchException($writersSchema, $readersSchema);
        }

        // Schema resolution: reader's schema is a union, writer's schema is not
        if (AvroSchema::UNION_SCHEMA == $readersSchema->getType()
            && AvroSchema::UNION_SCHEMA != $writersSchema->getType()) {
            foreach ($readersSchema->getSchemas() as $schema) {
                if (self::schemasMatch($writersSchema, $schema)) {
                    return $this->readData($writersSchema, $schema, $decoder);
                }
            }
            throw new AvroIOSchemaMatchException($writersSchema, $readersSchema);
        }

        switch ($writersSchema->getType()) {
            case AvroSchema::NULL_TYPE:
                return $decoder->readNull();
            case AvroSchema::BOOLEAN_TYPE:
                return $decoder->readBoolean();
            case AvroSchema::INT_TYPE:
                return $decoder->readInt();
            case AvroSchema::LONG_TYPE:
                return $decoder->readLong();
            case AvroSchema::FLOAT_TYPE:
                return $decoder->readFloat();
            case AvroSchema::DOUBLE_TYPE:
                return $decoder->readDouble();
            case AvroSchema::STRING_TYPE:
                return $decoder->readString();
            case AvroSchema::BYTES_TYPE:
                return $decoder->readBytes();
            case AvroSchema::ARRAY_SCHEMA:
                return $this->readArray($writersSchema, $readersSchema, $decoder);
            case AvroSchema::MAP_SCHEMA:
                return $this->readMap($writersSchema, $readersSchema, $decoder);
            case AvroSchema::UNION_SCHEMA:
                return $this->readUnion($writersSchema, $readersSchema, $decoder);
            case AvroSchema::ENUM_SCHEMA:
                return $this->readEnum($writersSchema, $readersSchema, $decoder);
            case AvroSchema::FIXED_SCHEMA:
                return $this->readFixed($writersSchema, $readersSchema, $decoder);
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
            case AvroSchema::REQUEST_SCHEMA:
                return $this->readRecord($writersSchema, $readersSchema, $decoder);
            default:
                throw new AvroException(sprintf(
                    "Cannot read unknown schema type: %s",
                    $writersSchema->getType()
                ));
        }
    }

    /**
     * @param AvroArraySchemaInterface|AvroSchemaInterface $writersSchema
     * @param AvroArraySchemaInterface|AvroSchemaInterface $readersSchema
     * @param IOBinaryDecoderInterface                     $decoder
     *
     * @return array
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function readArray($writersSchema, $readersSchema, $decoder)
    {
        $items       = [];
        $block_count = $decoder->readLong();
        while (0 != $block_count) {
            if ($block_count < 0) {
                $block_count = -$block_count;
                $decoder->readLong(); // Read (and ignore) block size
            }
            for ($i = 0; $i < $block_count; $i++) {
                $items [] = $this->readData($writersSchema->getItems(), $readersSchema->getItems(), $decoder);
            }
            $block_count = $decoder->readLong();
        }

        return $items;
    }

    /**
     * @param AvroMapSchemaInterface|AvroSchemaInterface $writersSchema
     * @param AvroMapSchemaInterface|AvroSchemaInterface $readersSchema
     * @param IOBinaryDecoderInterface                   $decoder
     *
     * @return array
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function readMap($writersSchema, $readersSchema, $decoder)
    {
        $items      = [];
        $pair_count = $decoder->readLong();
        while (0 != $pair_count) {
            if ($pair_count < 0) {
                $pair_count = -$pair_count;
                // Note: Ingoring what we read here
                $decoder->readLong();
            }

            for ($i = 0; $i < $pair_count; $i++) {
                $key         = $decoder->readString();
                $items[$key] = $this->readData($writersSchema->getValues(), $readersSchema->getValues(), $decoder);
            }
            $pair_count = $decoder->readLong();
        }

        return $items;
    }

    /**
     * @param AvroUnionSchemaInterface $writersSchema
     * @param AvroUnionSchemaInterface $readersSchema
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return mixed
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function readUnion($writersSchema, $readersSchema, $decoder)
    {
        $schema_index          = $decoder->readLong();
        $selectedWritersSchema = $writersSchema->getSchemaByIndex($schema_index);

        return $this->readData($selectedWritersSchema, $readersSchema, $decoder);
    }

    /**
     * @param AvroEnumSchemaInterface|AvroSchemaInterface $writersSchema
     * @param AvroEnumSchemaInterface|AvroSchemaInterface $readersSchema
     * @param IOBinaryDecoderInterface                    $decoder
     *
     * @return string
     */
    public function readEnum($writersSchema, $readersSchema, $decoder)
    {
        $symbol_index = $decoder->readInt();
        $symbol       = $writersSchema->getSymbolByIndex($symbol_index);
        if (!$readersSchema->hasSymbol($symbol)) {
            null;  // FIXME: unset wrt schema resolution
        }

        return $symbol;
    }

    /**
     * @param AvroFixedSchemaInterface|AvroSchemaInterface $writersSchema
     * @param AvroFixedSchemaInterface|AvroSchemaInterface $readersSchema
     * @param IOBinaryDecoderInterface                $decoder
     *
     * @return string
     */
    public function readFixed($writersSchema, $readersSchema, $decoder)
    {
        return $decoder->read($writersSchema->getSize());
    }

    /**
     * @param AvroRecordSchemaInterface|AvroSchemaInterface $writersSchema
     * @param AvroRecordSchemaInterface|AvroSchemaInterface $readersSchema
     * @param IOBinaryDecoderInterface                      $decoder
     *
     * @return array
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function readRecord($writersSchema, $readersSchema, $decoder)
    {
        $readersFields = $readersSchema->getFieldsHash();
        $record        = [];
        foreach ($writersSchema->getFields() as $writersField) {
            $type = $writersField->getType();
            if (isset($readersFields[$writersField->getName()])) {
                $record[$writersField->getName()] = $this->readData(
                    $type,
                    $readersFields[$writersField->getName()]->getType(),
                    $decoder
                );
            } else {
                $this->skipData($type, $decoder);
            }
        }
        // Fill in default values
        if (count($readersFields) > count($record)) {
            $writersFields = $writersSchema->getFieldsHash();
            foreach ($readersFields as $fieldName => $field) {
                if (!isset($writersFields[$fieldName])) {
                    if ($field->hasDefaultValue()) {
                        $record[$field->getName()] = $this->readDefaultValue(
                            $field->getType(),
                            $field->getDefaultValue()
                        );
                    } else {
                        null;  // FIXME: unset
                    }
                }
            }
        }

        return $record;
    }

    /**
     * @param AvroSchemaInterface|AvroArraySchemaInterface|AvroMapSchemaInterface|AvroUnionSchemaInterface|AvroRecordSchemaInterface|string $fieldSchema
     * @param null|boolean|int|float|string|array $defaultValue
     *
     * @return null|boolean|int|float|string|array
     *
     * @throws AvroException if $field_schema type is unknown.
     */
    public function readDefaultValue($fieldSchema, $defaultValue)
    {
        switch ($fieldSchema->getType()) {
            case AvroSchema::NULL_TYPE:
                return null;
            case AvroSchema::BOOLEAN_TYPE:
                return $defaultValue;
            case AvroSchema::INT_TYPE:
            case AvroSchema::LONG_TYPE:
                return (int) $defaultValue;
            case AvroSchema::FLOAT_TYPE:
            case AvroSchema::DOUBLE_TYPE:
                return (float) $defaultValue;
            case AvroSchema::STRING_TYPE:
            case AvroSchema::BYTES_TYPE:
                return $defaultValue;
            case AvroSchema::ARRAY_SCHEMA:
                $array = [];
                foreach ($defaultValue as $json_val) {
                    $val      = $this->readDefaultValue($fieldSchema->getItems(), $json_val);
                    $array [] = $val;
                }

                return $array;
            case AvroSchema::MAP_SCHEMA:
                $map = [];
                foreach ($defaultValue as $key => $json_val) {
                    $map[$key] = $this->readDefaultValue($fieldSchema->getValues(), $json_val);
                }

                return $map;
            case AvroSchema::UNION_SCHEMA:
                return $this->readDefaultValue($fieldSchema->getSchemaByIndex(0), $defaultValue);
            case AvroSchema::ENUM_SCHEMA:
            case AvroSchema::FIXED_SCHEMA:
                return $defaultValue;
            case AvroSchema::RECORD_SCHEMA:
                $record = [];
                foreach ($fieldSchema->getFields() as $field) {
                    $fieldName = $field->getName();
                    if (!$json_val = $defaultValue[$fieldName]) {
                        $json_val = $field->getDefaultValue();
                    }

                    $record[$fieldName] = $this->readDefaultValue($field->getType(), $json_val);
                }

                return $record;
            default:
                throw new AvroException(sprintf('Unknown type: %s', $fieldSchema->getType()));
        }
    }
}
