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

namespace Avro\Protocol;

use Avro\Exception\Protocol\AvroProtocolParseException;
use Avro\Exception\Schema\AvroSchemaParseException;
use Avro\Schema\AvroNamedSchemata;
use Avro\Schema\AvroNamedSchemataInterface;
use Avro\Schema\AvroSchema;

/**
 * Class AvroProtocol
 *
 * Avro library for protocols
 */
class AvroProtocol
{
    /** @var string */
    public $name;

    /** @var string */
    public $namespace;

    /** @var AvroNamedSchemataInterface */
    public $schemata;

    /** @var string */
    protected $protocol;

    /** @var mixed */
    protected $messages;

    /**
     * @param $avro
     *
     * @throws AvroSchemaParseException
     */
    protected function realParse($avro)
    {
        $this->protocol  = $avro["protocol"];
        $this->namespace = $avro["namespace"];
        $this->schemata  = new AvroNamedSchemata();
        $this->name      = $avro["protocol"];

        if (!is_null($avro["types"])) {
            $types = AvroSchema::realParse($avro["types"], $this->namespace, $this->schemata);
        }

        if (!is_null($avro["messages"])) {
            foreach ($avro["messages"] as $messageName => $messageAvro) {
                $message = new AvroProtocolMessage($messageName, $messageAvro, $this);

                $this->messages{$messageName} = $message;
            }
        }
    }

    /**
     * @param $json
     *
     * @return AvroProtocol
     * @throws AvroProtocolParseException
     * @throws AvroSchemaParseException
     */
    public static function parse($json)
    {
        if (is_null($json)) {
            throw new AvroProtocolParseException("Protocol can't be null");
        }

        $protocol = new AvroProtocol();
        $protocol->realParse(json_decode($json, true));

        return $protocol;
    }
}
