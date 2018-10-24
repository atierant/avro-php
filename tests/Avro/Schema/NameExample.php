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
 * Class NameExample
 */
class NameExample
{
    public $isValid;
    public $name;
    public $namespace;
    public $defaultNamespace;
    public $expectedFullname;

    /**
     * NameExample constructor.
     *
     * @param      $name
     * @param      $namespace
     * @param      $defaultNamespace
     * @param      $isValid
     * @param null $expectedFullname
     */
    public function __construct($name, $namespace, $defaultNamespace, $isValid, $expectedFullname = null)
    {
        $this->name             = $name;
        $this->namespace        = $namespace;
        $this->defaultNamespace = $defaultNamespace;
        $this->isValid          = $isValid;
        $this->expectedFullname = $expectedFullname;
    }

    /**
     * @return mixed
     */
    public function __toString()
    {
        return var_export($this, true);
    }
}
