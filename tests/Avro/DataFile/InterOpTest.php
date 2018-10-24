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

use Avro\Exception\AvroException;
use Avro\Exception\AvroNotImplementedException;
use Avro\Exception\Data\AvroDataIOException;
use Avro\Exception\Datum\AvroIOTypeException;
use Avro\Exception\IO\AvroIOException;
use Avro\Exception\Schema\AvroSchemaParseException;
use Avro\IO\AvroFile;
use Avro\Schema\AvroSchema;
use PHPUnit\Framework\TestCase;

/**
 * Class InterOpTest
 * @group interop
 */
class InterOpTest extends TestCase
{
    public $projectionJson;
    public $projection;

    public function setUp()
    {
        $interopSchemaFileName = AVRO_INTEROP_SCHEMA;
        $this->projectionJson  = file_get_contents($interopSchemaFileName);
        $this->projection      = AvroSchema::parse($this->projectionJson);
    }

    /**
     * @return array
     */
    public function fileNameProvider()
    {
        $dataDir   = '../Data';
        $dataFiles = [];

        if (!($dirHandle = opendir(dirname(__FILE__) . DIRECTORY_SEPARATOR . $dataDir))) {
            die("Could not open Data dir '$dataDir'\n");
        }

        /* TODO This currently only tries to read files of the form 'language.avro',
         * but not 'language_deflate.avro' as the PHP implementation is not yet
         * able to read deflate Data files. When deflate support is added, change
         * this to match *.avro. */
        while ($file = readdir($dirHandle)) {
            if (0 < preg_match('/^[a-z]+\.avro$/', $file)) {
                $dataFiles [] = join(DIRECTORY_SEPARATOR, [$dataDir, $file]);
            }
        }
        closedir($dirHandle);

        $ary = [];
        foreach ($dataFiles as $dataFile) {
            $ary[] = [$dataFile];
        }

        return $ary;
    }

    /**
     * @dataProvider fileNameProvider
     *
     * @param $fileName
     *
     * @throws AvroDataIOException
     * @throws AvroException
     * @throws AvroNotImplementedException
     * @throws AvroIOTypeException
     * @throws AvroIOException
     * @throws AvroSchemaParseException
     */
    public function testRead($fileName)
    {
        $dataIOReader = AvroDataIO::openFile($fileName, AvroFile::READ_MODE, $this->projectionJson);

        $data = $dataIOReader->data();

        self::assertNotEquals(0, count($data), sprintf("no Data read from %s", $fileName));

        foreach ($data as $idx => $datum) {
            self::assertNotNull($datum, sprintf("null datum from %s", $fileName));
        }
    }
}
