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

namespace MyNamespace;

require __DIR__ . '/../vendor/autoload.php';

use Avro\DataFile\AvroDataIO;
use Avro\DataFile\AvroDataIOReader;
use Avro\DataFile\AvroDataIOWriter;
use Avro\Datum\AvroIODatumReader;
use Avro\Datum\AvroIODatumWriter;
use Avro\IO\AvroStringIO;
use Avro\Schema\AvroSchema;

/**
 * Class WriteRead
 */
class WriteReadExample
{
    /**
     * @throws \Avro\Exception\AvroException
     * @throws \Avro\Exception\AvroNotImplementedException
     * @throws \Avro\Exception\Data\AvroDataIOException
     * @throws \Avro\Exception\Datum\AvroIOSchemaMatchException
     * @throws \Avro\Exception\Datum\AvroIOTypeException
     * @throws \Avro\Exception\IO\AvroIOException
     * @throws \Avro\Exception\Schema\AvroSchemaParseException
     */
    public function execute()
    {
        // Write and read a Data file
        $writersSchemaJson = <<<_JSON
            {"name":"member",
             "type":"record",
             "fields":[{"name":"member_id", "type":"int"},
                       {"name":"member_name", "type":"string"}]}
_JSON;

        $jose  = ['member_id' => 1392, 'member_name' => 'Jose'];
        $maria = ['member_id' => 1642, 'member_name' => 'Maria'];
        $data  = [$jose, $maria];

        $fileName = 'data.avr';
        // Open $fileName for writing, using the given writer's schema
        $dataWriter = AvroDataIO::openFile($fileName, 'w', $writersSchemaJson);

        // Write each datum to the file
        foreach ($data as $datum) {
            $dataWriter->append($datum);
        }
        // Tidy up
        $dataWriter->close();

        // Open $fileName (by default for reading) using the writer's schema included in the file
        $dataReader = AvroDataIO::openFile($fileName);
        echo "from file:\n";

        // Read each datum
        foreach ($dataReader->data() as $datum) {
            echo var_export($datum, true) . "\n";
        }
        $dataReader->close();

        // Create a Data string
        // Create a string io object.
        $io = new AvroStringIO();

        // Create a datum writer object
        $writersSchema = AvroSchema::parse($writersSchemaJson);
        $writer        = new AvroIODatumWriter($writersSchema);
        $dataWriter    = new AvroDataIOWriter($io, $writer, $writersSchema);
        foreach ($data as $datum) {
            $dataWriter->append($datum);
        }
        $dataWriter->close();

        $binaryString = $io->string();

        // Load the string Data string
        $readIO     = new AvroStringIO($binaryString);
        $dataReader = new AvroDataIOReader($readIO, new AvroIODatumReader());
        echo "from binary string:\n";
        foreach ($dataReader->data() as $datum) {
            echo var_export($datum, true) . "\n";
        }
    }
}

$writeRead = new WriteReadExample();
$writeRead->execute();

/** Output
 * from file:
 * array (
 * 'member_id' => 1392,
 * 'member_name' => 'Jose',
 * )
 * array (
 * 'member_id' => 1642,
 * 'member_name' => 'Maria',
 * )
 * from binary string:
 * array (
 * 'member_id' => 1392,
 * 'member_name' => 'Jose',
 * )
 * array (
 * 'member_id' => 1642,
 * 'member_name' => 'Maria',
 * )
 */
