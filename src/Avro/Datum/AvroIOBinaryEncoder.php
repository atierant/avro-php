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

use Avro\Avro;
use Avro\Exception\AvroException;
use Avro\GMP\AvroGMP;
use Avro\IO\IOInterface;

/**
 * Class AvroIOBinaryEncoder
 *
 * Encodes and writes Avro Data to an AvroIO object using Avro binary encoding.
 */
class AvroIOBinaryEncoder implements IOBinaryEncoderInterface
{
    /** @var IOInterface */
    private $io;

    /**
     * AvroIOBinaryEncoder constructor.
     *
     * @param IOInterface $io Object to which Data is to be written.
     *
     * @throws AvroException
     */
    public function __construct(IOInterface $io)
    {
        Avro::checkPlatform();
        $this->io = $io;
    }

    /**
     * Performs encoding of the given float value to a binary string
     *
     * XXX: This is <b>not</b> endian-aware! The {@link Avro::check_platform()}
     * called in {@link Avro\Datum\AvroIOBinaryEncoder::__construct()} should ensure the
     * library is only used on little-endian platforms, which ensure the little-endian
     * encoding required by the Avro spec.
     *
     * @param float $float
     *
     * @return string bytes
     * @see Avro::checkPlatform()
     */
    public static function floatToIntBits($float)
    {
        return pack('f', (float) $float);
    }

    /**
     * Performs encoding of the given double value to a binary string
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link Avro\Datum\AvroIOBinaryEncoder::float_to_int_bits()} for details.
     *
     * @param double $double
     *
     * @return string bytes
     */
    public static function doubleToLongBits($double)
    {
        return pack('d', (double) $double);
    }

    /**
     * @param int|string $n
     *
     * @return string long $n encoded as bytes
     * @internal This relies on 64-bit PHP.
     */
    public static function encodeLong($n)
    {
        $n   = (int) $n;
        $n   = ($n << 1) ^ ($n >> 63);
        $str = '';
        while (0 != ($n & ~0x7F)) {
            $str .= chr(($n & 0x7F) | 0x80);
            $n   >>= 7;
        }
        $str .= chr($n);

        return $str;
    }

    /**
     * @param null $datum actual value is ignored
     *
     * @return null
     */
    public function writeNull($datum)
    {
        return null;
    }

    /**
     * @param bool $datum
     */
    public function writeBoolean($datum)
    {
        $byte = $datum ? chr(1) : chr(0);
        $this->write($byte);
    }

    /**
     * @param int $datum
     */
    public function writeInt($datum)
    {
        $this->writeLong($datum);
    }

    /**
     * @param int $n
     */
    public function writeLong($n)
    {
        if (Avro::usesGMP()) {
            $this->write(AvroGMP::encodeLong($n));
        } else {
            $this->write(self::encodeLong($n));
        }
    }

    /**
     * @param float $datum
     *
     * @uses self::floatToIntBits()
     */
    public function writeFloat($datum)
    {
        $this->write(self::floatToIntBits($datum));
    }

    /**
     * @param float $datum
     *
     * @uses self::doubleToLongBits()
     */
    public function writeDouble($datum)
    {
        $this->write(self::doubleToLongBits($datum));
    }

    /**
     * @param string $str
     *
     * @uses self::writeBytes()
     */
    public function writeString($str)
    {
        $this->writeBytes($str);
    }

    /**
     * @param string $bytes
     */
    public function writeBytes($bytes)
    {
        $this->writeLong(strlen($bytes));
        $this->write($bytes);
    }

    /**
     * @param string $datum
     */
    public function write($datum)
    {
        $this->io->write($datum);
    }
}
