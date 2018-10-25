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

namespace Avro\Tests;

use Avro\DataFile\AvroDataIO;
use Avro\Exception\AvroException;
use Avro\Exception\AvroNotImplementedException;
use Avro\Exception\Data\AvroDataIOException;
use Avro\Exception\Datum\AvroIOTypeException;
use Avro\Exception\IO\AvroIOException;
use Avro\Exception\Schema\AvroSchemaParseException;

/**
 * Class InteropDataGenerator
 */
class InteropDataGenerator
{
    /**
     * InteropDataGenerator constructor.
     * @throws AvroException
     * @throws AvroNotImplementedException
     * @throws AvroDataIOException
     * @throws AvroIOTypeException
     * @throws AvroIOException
     * @throws AvroSchemaParseException
     */
    public function __construct()
    {
        $dataFile = join(DIRECTORY_SEPARATOR, [AVRO_BUILD_DATA_DIR, 'php.avro']);

        $datum = [
            'nullField'   => null,
            'boolField'   => true,
            'intField'    => -42,
            'longField'   => (int) 2147483650,
            'floatField'  => 1234.0,
            'doubleField' => -5432.6,
            'stringField' => 'hello avro',
            'bytesField'  => "\x16\xa6",
            'arrayField'  => [5.0, -6.0, -10.5],
            'mapField'    => ['a' => ['label' => 'a'],
                              'c' => ['label' => '3P0']],
            'unionField'  => 14.5,
            'enumField'   => 'C',
            'fixedField'  => '1019181716151413',
            'recordField' => [
                'label'    => 'blah',
                'children' => [
                    [
                        'label'    => 'inner',
                        'children' => [],
                    ],
                ],
            ],
        ];

        $schemaJson       = file_get_contents(AVRO_INTEROP_SCHEMA);
        $avroDataIOWriter = AvroDataIO::openFile($dataFile, 'w', $schemaJson);
        $avroDataIOWriter->append($datum);
        $avroDataIOWriter->close();
    }
}
