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

/**
 * Interface AvroNamedSchemataInterface
 *
 * Keeps track of AvroNamedSchema which have been observed so far, as well as the default namespace.
 */
interface AvroNamedSchemataInterface
{
    /**
     * List Schemas
     *
     * @return mixed
     */
    public function listSchemas();

    /**
     * @param string $fullname
     *
     * @return boolean true if there exists a schema with the given name and false otherwise.
     */
    public function hasName($fullname);

    /**
     * @param string $fullname
     *
     * @return AvroSchemaInterface|null The schema which has the given name,
     *                                   or null if there is no schema with the given name.
     */
    public function getSchema($fullname);

    /**
     * @param AvroNameInterface $name
     *
     * @return AvroSchemaInterface|null
     */
    public function getSchemaByName($name);

    /**
     * Creates a new AvroNamedSchemata instance of this schemata instance
     * with the given $schema appended.
     *
     * @param AvroNamedSchemaInterface $schema to add to this existing schemata
     *
     * @return AvroNamedSchemataInterface
     */
    public function cloneWithNewSchema($schema);
}
