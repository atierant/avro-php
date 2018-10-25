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

namespace Avro\IO;

use Avro\DataFile\AvroDataIOReader;
use Avro\DataFile\AvroDataIOWriter;
use Avro\Datum\AvroIODatumReader;
use Avro\Datum\AvroIODatumWriter;
use Avro\Debug\AvroDebug;
use Avro\Schema\AvroSchema;
use PHPUnit\Framework\TestCase;

/**
 * Class StringIOTest
 */
class StringIOTest extends TestCase
{

    public function testWrite()
    {
        $stringIO = new AvroStringIO();
        self::assertEquals(0, $stringIO->tell());
        $str    = 'foo';
        $strlen = strlen($str);
        self::assertEquals($strlen, $stringIO->write($str));
        self::assertEquals($strlen, $stringIO->tell());
    }

    public function testSeek()
    {
        self::markTestIncomplete('This test has not been implemented yet.');
    }

    public function testTell()
    {
        self::markTestIncomplete('This test has not been implemented yet.');
    }

    public function testRead()
    {
        self::markTestIncomplete('This test has not been implemented yet.');
    }

    public function testStringRep()
    {
        $writersSchemaJson = '"null"';
        $writersSchema      = AvroSchema::parse($writersSchemaJson);
        $datumWriter        = new AvroIODatumWriter($writersSchema);
        $strio               = new AvroStringIO();
        self::assertEquals('', $strio->string());
        $dataIOWriter = new AvroDataIOWriter($strio, $datumWriter, $writersSchemaJson);
        $dataIOWriter->close();

        self::assertEquals(57, strlen($strio->string()), AvroDebug::asciiString($strio->string()));

        $readStrIO = new AvroStringIO($strio->string());

        $datumReader = new AvroIODatumReader();
        $dataIOReader           = new AvroDataIOReader($readStrIO, $datumReader);
        $readData    = $dataIOReader->data();
        $datumCount  = count($readData);
        self::assertEquals(0, $datumCount);
    }
}
