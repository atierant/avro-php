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

namespace Avro\Exception\IO;

use Avro\Exception\AvroException;
use Avro\Schema\AvroSchemaInterface;
use Throwable;

/**
 * Class AvroSchemaResolutionException
 *
 * Exceptions associated with AvroIO instances.
 */
class AvroSchemaResolutionException extends AvroException
{
    /**
     * AvroSchemaResolutionException constructor.
     *
     * @param string $message
     * @param AvroSchemaInterface $writersSchema
     * @param AvroSchemaInterface|null $readersSchema
     * @param int $code
     * @param Throwable|null $previous
     */
    public function __construct(
        $message = "",
        AvroSchemaInterface $writersSchema = null,
        AvroSchemaInterface $readersSchema = null,
        $code = 0,
        Throwable $previous = null
    )
    {
        $prettyWriters = json_encode($writersSchema);
        $prettyReaders = json_encode($readersSchema);
        if ($writersSchema) {
            $message += sprintf("\nWriter's Schema: %s", $prettyWriters);
        }
        if ($readersSchema) {
            $message += sprintf("\nReader's Schema: %s" % $prettyReaders);
        }
        parent::__construct($message, $code, $previous);
    }
}
