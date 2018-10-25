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

use Avro\Exception\AvroException;
use Avro\Exception\Schema\AvroSchemaParseException;
use Avro\Util\AvroUtil;

/**
 * Class AvroEnumSchema
 */
class AvroEnumSchema extends AvroNamedSchema implements AvroEnumSchemaInterface
{
    /** @var string[] array of symbols */
    private $symbols;

    /**
     * @param AvroNameInterface          $name
     * @param string                     $doc
     * @param string[]                   $symbols
     * @param AvroNamedSchemataInterface &$schemata
     *
     * @throws AvroSchemaParseException
     */
    public function __construct($name, $doc, $symbols, &$schemata = null)
    {
        if (!AvroUtil::isList($symbols)) {
            throw new AvroSchemaParseException('Enum Schema symbols are not a list');
        }

        if (count(array_unique($symbols)) > count($symbols)) {
            throw new AvroSchemaParseException(sprintf('Duplicate symbols: %s', $symbols));
        }

        foreach ($symbols as $symbol) {
            if (!is_string($symbol) || empty($symbol)) {
                throw new AvroSchemaParseException(sprintf(
                    'Enum schema symbol must be a string %',
                    print_r($symbol, true)
                ));
            }
        }

        parent::__construct(AvroSchema::ENUM_SCHEMA, $name, $doc, $schemata);
        $this->symbols = $symbols;
    }

    /**
     * @return string[] this enum schema's symbols
     */
    public function getSymbols()
    {
        return $this->symbols;
    }

    /**
     * @param string $symbol
     *
     * @return boolean true if the given symbol exists in this enum schema and false otherwise
     */
    public function hasSymbol($symbol)
    {
        return in_array($symbol, $this->symbols);
    }

    /**
     * @param int $index
     *
     * @return string enum schema symbol with the given (zero-based) index
     * @throws AvroException
     */
    public function getSymbolByIndex($index)
    {
        if (array_key_exists($index, $this->symbols)) {
            return $this->symbols[$index];
        }

        throw new AvroException(sprintf('Invalid symbol index %d', $index));
    }

    /**
     * @param string $symbol
     *
     * @return int the index of the given $symbol in the enum schema
     * @throws AvroException
     */
    public function getSymbolIndex($symbol)
    {
        $idx = array_search($symbol, $this->symbols, true);
        if (false !== $idx) {
            return $idx;
        }

        throw new AvroException(sprintf("Invalid symbol value '%s'", $symbol));
    }

    /**
     * @return mixed
     */
    public function toAvro()
    {
        $avro = parent::toAvro();

        $avro[AvroSchema::SYMBOLS_ATTR] = $this->symbols;

        return $avro;
    }
}
