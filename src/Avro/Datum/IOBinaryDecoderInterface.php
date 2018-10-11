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

namespace Avro\Datum;

use Avro\IO\IOInterface;

/**
 * Interface IOBinaryDecoderInterface
 *
 * Decodes and reads Data from an IO object encoded using binary encoding.
 */
interface IOBinaryDecoderInterface
{
    /**
     * @return null
     */
    public function readNull();

    /**
     * @return boolean
     */
    public function readBoolean();

    /**
     * @return int
     */
    public function readInt();

    /**
     * @return int
     */
    public function readLong();

    /**
     * @return float
     */
    public function readFloat();

    /**
     * @return double
     */
    public function readDouble();

    /**
     * A string is encoded as a long followed by that many bytes of UTF-8 encoded character Data.
     *
     * @return string
     */
    public function readString();

    /**
     * @return string
     */
    public function readBytes();

    /**
     * @param int $len Count of bytes to read
     *
     * @return string
     */
    public function read($len);

    /**
     * @return mixed
     */
    public function skipNull();

    /**
     * @return mixed
     */
    public function skipBoolean();

    /**
     * @return mixed
     */
    public function skipInt();

    /**
     * @return mixed
     */
    public function skipFloat();

    /**
     * @return mixed
     */
    public function skipDouble();

    /**
     * @return mixed
     */
    public function skipBytes();

    /**
     * @return mixed
     */
    public function skipString();

    /**
     * @param int $len Count of bytes to skip
     *
     * @return bool
     *
     * @uses IOInterface::seek()
     */
    public function skip($len);
}
