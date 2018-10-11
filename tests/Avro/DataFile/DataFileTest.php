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

namespace Avro\DataFile;

use PHPUnit\Framework\TestCase;

/**
 * Class DataFileTest
 */
class DataFileTest extends TestCase
{
    const REMOVE_DATAFILES = true;

    private $dataFiles;

    /**
     * @param $dataFile
     *
     * @return string
     */
    protected function addDataFile($dataFile)
    {
        if (is_null($this->dataFiles)) {
            $this->dataFiles = [];
        }
        $dataFile           = "$dataFile." . self::currentTimestamp();
        $full               = join(DIRECTORY_SEPARATOR, [TEST_TEMP_DIR, $dataFile]);
        $this->dataFiles [] = $full;

        return $full;
    }

    /**
     * @param $dataFile
     */
    protected static function removeDataFile($dataFile)
    {
        if (file_exists($dataFile)) {
            unlink($dataFile);
        }
    }

    protected function removeDataFiles()
    {
        if (self::REMOVE_DATAFILES && $this->dataFiles) {
            foreach ($this->dataFiles as $dataFile) {
                self::removeDataFile($dataFile);
            }
        }
    }

    protected function setUp()
    {
        if (!file_exists(TEST_TEMP_DIR)) {
            mkdir(TEST_TEMP_DIR);
        }
        $this->removeDataFiles();
    }

    protected function tearDown()
    {
        $this->removeDataFiles();
    }

    /**
     * @return string
     */
    public static function currentTimestamp()
    {
        return strftime("%Y%m%dT%H%M%S");
    }

    public function testWriteReadNothingRoundTrip()
    {
        $dataFile      = $this->addDataFile('data-wr-nothing-null.avr');
        $writersSchema = '"null"';

        $dataIOWriter = AvroDataIO::openFile($dataFile, 'w', $writersSchema);
        $dataIOWriter->close();

        $dataIOReader = AvroDataIO::openFile($dataFile);
        $data         = $dataIOReader->data();
        $dataIOReader->close();

        self::assertEmpty($data);
    }

    public function testWriteReadNullRoundTrip()
    {
        $dataFile      = $this->addDataFile('data-wr-null.avr');
        $writersSchema = '"null"';
        $data          = null;

        $dataIOWriter = AvroDataIO::openFile($dataFile, 'w', $writersSchema);
        $dataIOWriter->append($data);
        $dataIOWriter->close();

        $dataIOReader = AvroDataIO::openFile($dataFile);
        $readData     = $dataIOReader->data();
        $readData     = reset($readData);
        $dataIOReader->close();

        self::assertEquals($data, $readData);
    }

    public function testWriteReadStringRoundTrip()
    {
        $dataFile      = $this->addDataFile('data-wr-str.avr');
        $writersSchema = '"string"';
        $data          = 'foo';

        $dataIOWriter = AvroDataIO::openFile($dataFile, 'w', $writersSchema);
        $dataIOWriter->append($data);
        $dataIOWriter->close();

        $dataIOReader = AvroDataIO::openFile($dataFile);
        $readData     = $dataIOReader->data();
        $readData     = reset($readData);
        $dataIOReader->close();

        self::assertEquals($data, $readData);
    }


    public function testWriteReadIntRoundTrip()
    {
        $dataFile      = $this->addDataFile('data-wr-int.avr');
        $writersSchema = '"int"';
        $data          = 1;

        $dataIOWriter = AvroDataIO::openFile($dataFile, 'w', $writersSchema);
        $dataIOWriter->append(1);
        $dataIOWriter->close();

        $dataIOReader = AvroDataIO::openFile($dataFile);
        $readData     = $dataIOReader->data();
        $readData     = reset($readData);
        $dataIOReader->close();

        self::assertEquals($data, $readData);
    }

    public function testWriteReadTrueRoundTrip()
    {
        $dataFile      = $this->addDataFile('data-wr-true.avr');
        $writersSchema = '"boolean"';
        $datum         = true;

        $dataIOWriter = AvroDataIO::openFile($dataFile, 'w', $writersSchema);
        $dataIOWriter->append($datum);
        $dataIOWriter->close();

        $dataIOReader = AvroDataIO::openFile($dataFile);
        $readData     = $dataIOReader->data();
        $readData     = reset($readData);
        $dataIOReader->close();

        self::assertEquals($datum, $readData);
    }

    public function testWriteReadFalseRoundTrip()
    {
        $dataFile      = $this->addDataFile('data-wr-false.avr');
        $writersSchema = '"boolean"';
        $datum         = false;

        $dataIOWriter = AvroDataIO::openFile($dataFile, 'w', $writersSchema);
        $dataIOWriter->append($datum);
        $dataIOWriter->close();

        $dataIOReader = AvroDataIO::openFile($dataFile);
        $readData     = $dataIOReader->data();
        $readData     = reset($readData);
        $dataIOReader->close();

        self::assertEquals($datum, $readData);
    }

    public function testWriteReadIntArrayRoundTrip()
    {
        $dataFile      = $this->addDataFile('data-wr-int-ary.avr');
        $writersSchema = '"int"';
        $data          = [10, 20, 30, 40, 50, 60, 70, 567, 89012345];

        $dataIOWriter = AvroDataIO::openFile($dataFile, 'w', $writersSchema);
        foreach ($data as $datum) {
            $dataIOWriter->append($datum);
        }
        $dataIOWriter->close();

        $dataIOReader = AvroDataIO::openFile($dataFile);
        $readData     = $dataIOReader->data();
        $dataIOReader->close();

        self::assertEquals($data, $readData, sprintf("in: %s\nout: %s", json_encode($data), json_encode($readData)));
    }

    public function testDifferingSchemasWithPrimitives()
    {
        $dataFile = $this->addDataFile('data-prim.avr');

        $writerSchema = <<<JSON
{ "type": "record",
  "name": "User",
  "fields" : [
      {"name": "username", "type": "string"},
      {"name": "age", "type": "int"},
      {"name": "verified", "type": "boolean", "default": "false"}
      ]}
JSON;

        $data = [
            ['username' => 'john', 'age' => 25, 'verified' => true],
            ['username' => 'ryan', 'age' => 23, 'verified' => false],
        ];

        $dataIOWriter = AvroDataIO::openFile($dataFile, 'w', $writerSchema);
        foreach ($data as $datum) {
            $dataIOWriter->append($datum);
        }
        $dataIOWriter->close();

        $readerSchema = <<<JSON
      { "type": "record",
        "name": "User",
        "fields" : [
      {"name": "username", "type": "string"}
      ]}
JSON;
        $dataIOReader = AvroDataIO::openFile($dataFile, 'r', $readerSchema);
        foreach ($dataIOReader->data() as $index => $record) {
            self::assertEquals($data[$index]['username'], $record['username']);
        }
    }

    public function testDifferingSchemasWithComplexObjects()
    {
        $dataFile = $this->addDataFile('data-complex.avr');

        $writersSchema = <<<JSON
{ "type": "record",
  "name": "something",
  "fields": [
    {"name": "something_fixed", "type": {"name": "inner_fixed",
                                         "type": "fixed", "size": 3}},
    {"name": "something_enum", "type": {"name": "inner_enum",
                                        "type": "enum",
                                        "symbols": ["hello", "goodbye"]}},
    {"name": "something_array", "type": {"type": "array", "items": "int"}},
    {"name": "something_map", "type": {"type": "map", "values": "int"}},
    {"name": "something_record", "type": {"name": "inner_record",
                                          "type": "record",
                                          "fields": [
                                            {"name": "inner", "type": "int"}
                                          ]}},
    {"name": "username", "type": "string"}
]}
JSON;

        $data = [
            [
                "username"         => "john",
                "something_fixed"  => "foo",
                "something_enum"   => "hello",
                "something_array"  => [1, 2, 3],
                "something_map"    => ["a" => 1, "b" => 2],
                "something_record" => ["inner" => 2],
                "something_error"  => ["code" => 403],
            ],
            [
                "username"         => "ryan",
                "something_fixed"  => "bar",
                "something_enum"   => "goodbye",
                "something_array"  => [1, 2, 3],
                "something_map"    => ["a" => 2, "b" => 6],
                "something_record" => ["inner" => 1],
                "something_error"  => ["code" => 401],
            ],
        ];

        $dataIOWriter = AvroDataIO::openFile($dataFile, 'w', $writersSchema);
        foreach ($data as $datum) {
            $dataIOWriter->append($datum);
        }
        $dataIOWriter->close();

        foreach (['fixed', 'enum', 'record', 'error', 'array', 'map', 'union'] as $s) {
            $readersSchema = json_decode($writersSchema, true);
            $dataIOReader  = AvroDataIO::openFile($dataFile, 'r', json_encode($readersSchema));
            foreach ($dataIOReader->data() as $idx => $obj) {
                foreach ($readersSchema['fields'] as $field) {
                    $fieldName = $field['name'];
                    self::assertEquals($data[$idx][$fieldName], $obj[$fieldName]);
                }
            }
            $dataIOReader->close();
        }
    }
}
