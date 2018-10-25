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
use Avro\Exception\IO\AvroSchemaResolutionException;
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
     * @param IOBinaryDecoderInterface $decoder
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
                return $this->skipArray($writersSchema, $decoder);
            case AvroSchema::MAP_SCHEMA:
                return $this->skipMap($writersSchema, $decoder);
            case AvroSchema::UNION_SCHEMA:
                return $this->skipUnion($writersSchema, $decoder);
            case AvroSchema::ENUM_SCHEMA:
                return $this->skipEnum($writersSchema, $decoder);
            case AvroSchema::FIXED_SCHEMA:
                return $this->skipFixed($writersSchema, $decoder);
            case AvroSchema::RECORD_SCHEMA:
            case AvroSchema::ERROR_SCHEMA:
            case AvroSchema::REQUEST_SCHEMA:
                return $this->skipRecord($writersSchema, $decoder);
            default:
                throw new AvroException(sprintf('Uknown schema type: %s', $writersSchema->getType()));
        }
    }

    /**
     * @param AvroArraySchemaInterface|AvroSchemaInterface $writersSchema
     * @param IOBinaryDecoderInterface $decoder
     * @throws AvroException
     */
    private function skipArray(AvroArraySchemaInterface $writersSchema, IOBinaryDecoderInterface $decoder)
    {
        $blockCount = $decoder->readLong();
        while (0 !== $blockCount) {
            if (0 > $blockCount) {
                $decoder->skip($decoder->readLong());
            } else {
                foreach (range(0, $blockCount) as $i) {
                    $this->skipData($writersSchema->getItems(), $decoder);
                }

            }

            $blockCount = $decoder->readLong();
        }
    }

    /**
     * @param AvroRecordSchemaInterface|AvroSchemaInterface $writersSchema
     * @param IOBinaryDecoderInterface $decoder
     * @throws AvroException
     */
    private function skipRecord(AvroRecordSchemaInterface $writersSchema, IOBinaryDecoderInterface $decoder)
    {
        foreach ($writersSchema->getFields() as $field) {
            $this->skipData($field->getType(), $decoder);
        }

        return;
    }

    /**
     * @param AvroMapSchemaInterface|AvroSchemaInterface $writersSchema
     * @param IOBinaryDecoderInterface $decoder
     * @throws AvroException
     */
    private function skipMap(AvroMapSchemaInterface $writersSchema, IOBinaryDecoderInterface $decoder)
    {
        $blockCount = $decoder->readLong();
        while (0 !== $blockCount) {
            if (0 > $blockCount) {
                $decoder->skip($decoder->readLong());
            } else {
                foreach (range(0, $blockCount) as $i) {
                    $decoder->skipBytes();
                    $this->skipData($writersSchema->getValues(), $decoder);
                }

            }

            $blockCount = $decoder->readLong();
        }
    }

    /**
     * @param AvroUnionSchemaInterface|AvroSchemaInterface $writersSchema
     * @param IOBinaryDecoderInterface $decoder
     * @return mixed
     * @throws AvroException
     */
    private function skipUnion(AvroUnionSchemaInterface $writersSchema, IOBinaryDecoderInterface $decoder)
    {
        $schemaIndex = $decoder->readLong();
        if ($schemaIndex >= count($writersSchema->getSchemas())) {
            $failMsg = sprintf("Can't access branch index %d for union with %d branches",
                $schemaIndex,
                count($writersSchema->getSchemas())
            );
            throw new AvroSchemaResolutionException($failMsg, $writersSchema);
        }
        return $this->skipData($writersSchema->getSchemaByIndex($schemaIndex), $decoder);
    }

    /**
     * @param AvroEnumSchemaInterface|AvroSchemaInterface $writersSchema
     * @param IOBinaryDecoderInterface $decoder
     * @return mixed
     */
    private function skipEnum(AvroEnumSchemaInterface $writersSchema, IOBinaryDecoderInterface $decoder)
    {
        return $decoder->skipInt();
    }

    /**
     * @param AvroFixedSchemaInterface|AvroSchemaInterface $writersSchema
     * @param IOBinaryDecoderInterface $decoder
     * @return bool
     */
    private function skipFixed(AvroFixedSchemaInterface $writersSchema, IOBinaryDecoderInterface $decoder)
    {
        return $decoder->skip($writersSchema->getSize());
    }

    /**
     * @param IOBinaryDecoderInterface $decoder
     * @param AvroArraySchemaInterface $writersSchema
     * @throws AvroException
     */
    private function skipBlocks(IOBinaryDecoderInterface $decoder, AvroArraySchemaInterface $writersSchema)
    {
        $blockCount = $decoder->readLong();
        while (0 !== $blockCount) {
            if (0 > $blockCount) {
                $decoder->skip($decoder->readLong());
            } else {
                // .rb => block_count.times &blk
                foreach (range(0, $blockCount) as $i) {
                    $this->skipData($writersSchema->getItems(), $decoder);
                }

            }

            $blockCount = $decoder->readLong();
        }
    }

    /**
     * Checks equivalence of the given attributes of the two given schemas.
     *
     * @param AvroSchemaInterface $schemaOne
     * @param AvroSchemaInterface $schemaTwo
     * @param string[] $attributeNames array of string attribute names to compare
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
     * @param IOBinaryDecoderInterface $decoder
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
     * Arrays are encoded as a series of blocks.
     *
     * Each block consists of a long count value, followed by that many array items.
     * A block with count zero indicates the end of the array. Each item is encoded per the array's item schema.
     *
     * If a block's count is negative, then the count is followed immediately by a long block size,
     * indicating the number of bytes in the block.
     * The actual count in this case is the absolute value of the count written.
     *
     * @param AvroArraySchemaInterface|AvroSchemaInterface $writersSchema
     * @param AvroArraySchemaInterface|AvroSchemaInterface $readersSchema
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return array
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function readArray($writersSchema, $readersSchema, $decoder)
    {
        $items = [];
        $blockCount = $decoder->readLong();
        while (0 != $blockCount) {
            if ($blockCount < 0) {
                $blockCount = -$blockCount;
                $decoder->readLong(); // Read (and ignore) block size
            }
            for ($i = 0; $i < $blockCount; $i++) {
                $items [] = $this->readData($writersSchema->getItems(), $readersSchema->getItems(), $decoder);
            }
            $blockCount = $decoder->readLong();
        }

        return $items;
    }

    /**
     * Maps are encoded as a series of blocks.
     *
     * Each block consists of a long count value, followed by that many key/value pairs.
     * A block with count zero indicates the end of the map.
     * Each item is encoded per the map's value schema.
     *
     * If a block's count is negative, then the count is followed immediately by a long block size,
     * indicating the number of bytes in the block.
     * The actual count in this case is the absolute value of the count written.
     *
     * @param AvroMapSchemaInterface|AvroSchemaInterface $writersSchema
     * @param AvroMapSchemaInterface|AvroSchemaInterface $readersSchema
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return array
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function readMap($writersSchema, $readersSchema, $decoder)
    {
        $items = [];
        $pairCount = $decoder->readLong();
        while (0 != $pairCount) {
            if ($pairCount < 0) {
                $pairCount = -$pairCount;
                // Note: Ingoring what we read here
                $decoder->readLong();
            }

            for ($i = 0; $i < $pairCount; $i++) {
                $key = $decoder->readString();
                $items[$key] = $this->readData($writersSchema->getValues(), $readersSchema->getValues(), $decoder);
            }
            $pairCount = $decoder->readLong();
        }

        return $items;
    }

    /**
     * A union is encoded by first writing a long value indicating
     * the zero-based position within the union of the schema of its value.
     * The value is then encoded per the indicated schema within the union.
     *
     * @param AvroUnionSchemaInterface|AvroSchemaInterface $writersSchema
     * @param AvroUnionSchemaInterface|AvroSchemaInterface $readersSchema
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return mixed
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function readUnion($writersSchema, $readersSchema, $decoder)
    {
        $schemaIndex = $decoder->readLong();
        $selectedWritersSchema = $writersSchema->getSchemaByIndex($schemaIndex);

        return $this->readData($selectedWritersSchema, $readersSchema, $decoder);
    }

    /**
     * An enum is encoded by a int, representing the zero-based position of the symbol in the schema.
     *
     * @param AvroEnumSchemaInterface|AvroSchemaInterface $writersSchema
     * @param AvroEnumSchemaInterface|AvroSchemaInterface $readersSchema
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return string
     */
    public function readEnum($writersSchema, $readersSchema, $decoder)
    {
        $symbolIndex = $decoder->readInt();
        $symbol = $writersSchema->getSymbolByIndex($symbolIndex);
        if (!$readersSchema->hasSymbol($symbol)) {
            null;  // FIXME: unset wrt schema resolution
        }

        return $symbol;
    }

    /**
     * Fixed instances are encoded using the number of bytes declared in the schema.
     *
     * @param AvroFixedSchemaInterface|AvroSchemaInterface $writersSchema
     * @param AvroFixedSchemaInterface|AvroSchemaInterface $readersSchema
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return string
     */
    public function readFixed($writersSchema, $readersSchema, $decoder)
    {
        return $decoder->read($writersSchema->getSize());
    }

    /**
     * A record is encoded by encoding the values of its fields
     * in the order that they are declared. In other words, a record
     * is encoded as just the concatenation of the encodings of its fields.
     * Field values are encoded per their schema.
     *
     * Schema Resolution:
     * - the ordering of fields may be different: fields are matched by name.
     * schemas for fields with the same name in both records are resolved
     * recursively.
     * - if the writer's record contains a field with a name not present in the
     * reader's record, the writer's value for that field is ignored.
     * - if the reader's record schema has a field that contains a default value,
     * and writer's schema does not have a field with the same name, then the
     * reader should use the default value from its field.
     * - if the reader's record schema has a field with no default value, and
     * writer's schema does not have a field with the same name, then the
     * field's value is unset.
     *
     * @param AvroRecordSchemaInterface|AvroSchemaInterface $writersSchema
     * @param AvroRecordSchemaInterface|AvroSchemaInterface $readersSchema
     * @param IOBinaryDecoderInterface $decoder
     *
     * @return array
     * @throws AvroException
     * @throws AvroIOSchemaMatchException
     */
    public function readRecord($writersSchema, $readersSchema, $decoder)
    {
        $readersFields = $readersSchema->getFieldsHash();
        $record = [];
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
                return (int)$defaultValue;
            case AvroSchema::FLOAT_TYPE:
            case AvroSchema::DOUBLE_TYPE:
                return (float)$defaultValue;
            case AvroSchema::STRING_TYPE:
            case AvroSchema::BYTES_TYPE:
                return $defaultValue;
            case AvroSchema::ARRAY_SCHEMA:
                $array = [];
                foreach ($defaultValue as $json_val) {
                    $val = $this->readDefaultValue($fieldSchema->getItems(), $json_val);
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
