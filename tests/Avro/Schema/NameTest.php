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

use Avro\Exception\Schema\AvroSchemaParseException;
use PHPUnit\Framework\TestCase;

require_once __DIR__ . '/NameExample.php';

/**
 * Class NameTest
 */
class NameTest extends TestCase
{
    /**
     * @return array
     */
    public function fullnameProvider()
    {
        $examples = [
            new NameExample('foo', null, null, true, 'foo'),
            new NameExample('foo', 'bar', null, true, 'bar.foo'),
            new NameExample('bar.foo', 'baz', null, true, 'bar.foo'),
            new NameExample('_bar.foo', 'baz', null, true, '_bar.foo'),
            new NameExample('bar._foo', 'baz', null, true, 'bar._foo'),
            new NameExample('3bar.foo', 'baz', null, false),
            new NameExample('bar.3foo', 'baz', null, false),
            new NameExample('b4r.foo', 'baz', null, true, 'b4r.foo'),
            new NameExample('bar.f0o', 'baz', null, true, 'bar.f0o'),
            new NameExample(' .foo', 'baz', null, false),
            new NameExample('bar. foo', 'baz', null, false),
            new NameExample('bar. ', 'baz', null, false),
        ];
        $exes     = [];
        foreach ($examples as $ex) {
            $exes [] = [$ex];
        }

        return $exes;
    }

    /**
     * @dataProvider fullnameProvider
     *
     * @param $ex
     */
    public function testFullname($ex)
    {
        try {
            $name = new AvroName($ex->name, $ex->namespace, $ex->defaultNamespace);
            self::assertTrue($ex->isValid);
            self::assertEquals($ex->expectedFullname, $name->getFullname());
        } catch (AvroSchemaParseException $e) {
            self::assertFalse($ex->isValid, sprintf("%s:\n%s", $ex, $e->getMessage()));
        }
    }

    /**
     * @return array
     */
    public function nameProvider()
    {
        return [
            ['a', true],
            ['_', true],
            ['1a', false],
            ['', false],
            [null, false],
            [' ', false],
            ['Cons', true],
        ];
    }

    /**
     * @dataProvider nameProvider
     *
     * @param $name
     * @param $isWellFormed
     */
    public function testName($name, $isWellFormed)
    {
        self::assertEquals(AvroName::isWellFormedName($name), $isWellFormed, $name);
    }
}
