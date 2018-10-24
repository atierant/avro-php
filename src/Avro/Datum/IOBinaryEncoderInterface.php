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

/**
 * Interface IOBinaryEncoderInterface
 *
 * Encodes and writes Data to an IO object using binary encoding.
 */
interface IOBinaryEncoderInterface
{
    /**
     * @param null $datum actual value is ignored
     *
     * @return null
     */
    public function writeNull($datum);

    /**
     * @param boolean $datum
     */
    public function writeBoolean($datum);

    /**
     * @param int $datum
     */
    public function writeInt($datum);

    /**
     * @param int $n
     */
    public function writeLong($n);

    /**
     * @param float $datum
     *
     * @uses self::floatToIntBits()
     */
    public function writeFloat($datum);

    /**
     * @param float $datum
     *
     * @uses self::doubleToLongBits()
     */
    public function writeDouble($datum);

    /**
     * @param string $str
     *
     * @uses self::writeBytes()
     */
    public function writeString($str);

    /**
     * @param string $bytes
     */
    public function writeBytes($bytes);

    /**
     * @param string $datum
     */
    public function write($datum);
}
