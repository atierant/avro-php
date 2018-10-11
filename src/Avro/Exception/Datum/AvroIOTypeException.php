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

namespace Avro\Exception\Datum;

use Avro\Exception\AvroException;
use Avro\Schema\AvroSchemaInterface;

/**
 * Class AvroIOTypeException
 *
 * Exceptions arising from writing or reading Avro Data.
 */
class AvroIOTypeException extends AvroException
{
    /**
     * AvroIOTypeException constructor.
     *
     * @param AvroSchemaInterface $expectedSchema
     * @param mixed               $datum
     */
    public function __construct(AvroSchemaInterface $expectedSchema, $datum)
    {
        parent::__construct(sprintf(
            'The datum %s is not an example of schema %s',
            var_export($datum, true),
            $expectedSchema
        ));
    }
}
