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

use Avro\Exception\Schema\AvroSchemaParseException;
use Avro\Schema\AvroName;
use Avro\Schema\AvroPrimitiveSchema;
use Avro\Schema\AvroPrimitiveSchemaInterface;
use Avro\Schema\AvroRecordSchema;
use Avro\Schema\AvroRecordSchemaInterface;
use Avro\Schema\AvroSchema;

/**
 * Class AvroProtocolMessage
 */
class AvroProtocolMessage
{
    /** @var AvroRecordSchemaInterface $request */
    public $request;

    /** @var AvroPrimitiveSchemaInterface */
    public $response;

    /** @var string */
    protected $name;

    /**
     * AvroProtocolMessage constructor.
     *
     * @param string       $name
     * @param mixed        $avro
     * @param AvroProtocol $protocol
     *
     * @throws AvroSchemaParseException
     */
    public function __construct($name, $avro, $protocol)
    {
        $this->name = $name;

        $this->request = new AvroRecordSchema(
            new AvroName($name, null, $protocol->namespace),
            null,
            $avro{'request'},
            $protocol->schemata,
            AvroSchema::REQUEST_SCHEMA
        );

        if (array_key_exists('response', $avro)) {
            $this->response = $protocol->schemata->getSchemaByName(new AvroName(
                $avro{'response'},
                $protocol->namespace,
                $protocol->namespace
            ));
            if ($this->response == null) {
                $this->response = new AvroPrimitiveSchema($avro{'response'});
            }
        }
    }
}
