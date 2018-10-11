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

namespace Avro;

use Avro\Debug\AvroDebug;

/**
 * Class Avro
 *
 * Library-level class for PHP Avro port.
 *
 * Contains library details such as version number and platform checks.
 *
 * This port is an implementation of the
 * {@link http://avro.apache.org/docs/1.3.3/spec.html Avro 1.3.3 Specification}
 */
class Avro
{
    /** @var string version number of Avro specification to which this implemenation complies */
    const SPEC_VERSION = '1.3.3';

    /**
     * Constant to enumerate endianness.
     * @access private
     * @var int
     */
    const BIG_ENDIAN    = 0x00;
    const LITTLE_ENDIAN = 0x01;
    /**
     * Constant to enumerate biginteger handling mode.
     * GMP is used, if available, on 32-bit platforms.
     */
    const PHP_BIGINTEGER_MODE = 0x00;
    const GMP_BIGINTEGER_MODE = 0x01;

    /**
     * Memoized result of self::setEndianness()
     * @var int self::BIG_ENDIAN or self::LITTLE_ENDIAN
     * @see self::setEndianness()
     */
    private static $endianness;

    /**
     * @var int
     * Mode used to handle bigintegers. After Avro::check64Bit() has been called, (usually via a call to
     * Avro::checkPlatform(), set to self::GMP_BIGINTEGER_MODE on 32-bit platforms that have GMP available,
     * and to self::PHP_BIGINTEGER_MODE otherwise.
     */
    private static $bigintegerMode;

    /**
     * Determines if the host platform can encode and decode long integer Data.
     *
     * @throws Exception\AvroException if the platform cannot handle long integers.
     */
    private static function check64Bit()
    {
        if (8 != PHP_INT_SIZE) {
            if (extension_loaded('gmp')) {
                self::$bigintegerMode = self::GMP_BIGINTEGER_MODE;
            } else {
                throw new Exception\AvroException('This platform cannot handle a 64-bit operations. '
                    . 'Please install the GMP PHP extension.');
            }
        } else {
            self::$bigintegerMode = self::PHP_BIGINTEGER_MODE;
        }
    }

    /**
     * Determines if the host platform is little endian, required for processing double and float Data.
     *
     * @throws Exception\AvroException if the platform is not little endian.
     */
    private static function checkLittleEndian()
    {
        if (!self::isLittleEndianPlatform()) {
            throw new Exception\AvroException('This is not a little-endian platform');
        }
    }

    /**
     * Determines the endianness of the host platform and memoizes
     * the result to Avro::$endianness.
     *
     * Based on a similar check perfomed in http://pear.php.net/package/Math_BinaryUtils
     *
     * @throws Exception\AvroException if the endianness cannot be determined.
     */
    private static function setEndianness()
    {
        $packed = pack('d', 1);
        switch ($packed) {
            case "\77\360\0\0\0\0\0\0":
                self::$endianness = self::BIG_ENDIAN;
                break;
            case "\0\0\0\0\0\0\360\77":
                self::$endianness = self::LITTLE_ENDIAN;
                break;
            default:
                throw new Exception\AvroException(sprintf(
                    'Error determining platform endianness: %s',
                    AvroDebug::hexString($packed)
                ));
        }
    }

    /**
     * @return boolean true if the host platform is big endian and false otherwise.
     * @throws Exception\AvroException
     * @uses self::setEndianness()
     */
    private static function isBigEndianPlatform()
    {
        if (is_null(self::$endianness)) {
            self::setEndianness();
        }

        return self::BIG_ENDIAN == self::$endianness;
    }

    /**
     * @return boolean true if the host platform is little endian, and false otherwise.
     * @throws Exception\AvroException
     * @uses self::isBigEndianPlatform()
     */
    private static function isLittleEndianPlatform()
    {
        return !self::isBigEndianPlatform();
    }

    /**
     * Wrapper method to call each required check.
     *
     * @throws Exception\AvroException
     */
    public static function checkPlatform()
    {
        self::check64Bit();
        self::checkLittleEndian();
    }

    /**
     * @return boolean true if the PHP GMP extension is used and false otherwise.
     * @internal Requires Avro::check_64_bit() (exposed via Avro::check_platform())
     *           to have been called to set Avro::$biginteger_mode.
     */
    public static function usesGMP()
    {
        return self::GMP_BIGINTEGER_MODE == self::$bigintegerMode;
    }
}
