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
 * Class AvroIOBinaryDecoder
 *
 * Decodes and reads Avro Data from an AvroIO object encoded using Avro binary encoding.
 */
class AvroIOBinaryDecoder implements IOBinaryDecoderInterface
{
    /** @var IOInterface */
    private $io;

    /**
     * AvroIOBinaryDecoder constructor.
     *
     * @param IOInterface $io Object from which to read.
     *
     * @throws AvroException
     */
    public function __construct(IOInterface $io)
    {
        Avro::checkPlatform();
        $this->io = $io;
    }

    /**
     * @return string The next byte from $this->io.
     */
    private function nextByte()
    {
        return $this->read(1);
    }

    /**
     * @return int position of pointer in Avro\IO\AvroIO instance
     *
     * @uses IOInterface::tell()
     */
    private function tell()
    {
        return $this->io->tell();
    }

    /**
     * @param int $offset
     * @param int $whence
     *
     * @return boolean true upon success
     * @uses IOInterface::seek()
     */
    private function seek($offset, $whence)
    {
        return $this->io->seek($offset, $whence);
    }

    /**
     * Skip Long
     */
    protected function skipLong()
    {
        $b = ord($this->nextByte());
        while (0 != ($b & 0x80)) {
            $b = ord($this->nextByte());
        }
    }

    /**
     * @param int[] array of byte ascii values
     *
     * @return int decoded value
     * @internal Requires 64-bit platform
     */
    public static function decodeLongFromArray($bytes)
    {
        $b     = array_shift($bytes);
        $n     = $b & 0x7f;
        $shift = 7;
        while (0 != ($b & 0x80)) {
            $b     = array_shift($bytes);
            $n     |= (($b & 0x7f) << $shift);
            $shift += 7;
        }

        return (($n >> 1) ^ -($n & 1));
    }

    /**
     * Performs decoding of the binary string to a float value.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link Avro\Datum\AvroIOBinaryEncoder::floatToIntBits()} for details.
     *
     * @param string $bits
     *
     * @return float
     */
    public static function intBitsToFloat($bits)
    {
        $float = unpack('f', $bits);

        return (float) $float[1];
    }

    /**
     * Performs decoding of the binary string to a double value.
     *
     * XXX: This is <b>not</b> endian-aware! See comments in
     * {@link Avro\Datum\AvroIOBinaryEncoder::floatToIntBits()} for details.
     *
     * @param string $bits
     *
     * @return float
     */
    public static function longBitsToDouble($bits)
    {
        $double = unpack('d', $bits);

        return (double) $double[1];
    }

    /**
     * @return null
     */
    public function readNull()
    {
        return null;
    }

    /**
     * @return bool
     */
    public function readBoolean()
    {
        return (boolean) (1 == ord($this->nextByte()));
    }

    /**
     * @return int
     */
    public function readInt()
    {
        return (int) $this->readLong();
    }

    /**
     * @return int|string
     */
    public function readLong()
    {
        $byte  = ord($this->nextByte());
        $bytes = [$byte];
        while (0 != ($byte & 0x80)) {
            $byte     = ord($this->nextByte());
            $bytes [] = $byte;
        }

        if (Avro::usesGMP()) {
            return AvroGMP::decodeLongFromArray($bytes);
        }

        return self::decodeLongFromArray($bytes);
    }

    /**
     * @return float
     */
    public function readFloat()
    {
        return self::intBitsToFloat($this->read(4));
    }

    /**
     * @return float
     */
    public function readDouble()
    {
        return self::longBitsToDouble($this->read(8));
    }

    /**
     * A string is encoded as a long followed by that many bytes of UTF-8 encoded character Data.
     *
     * @return string
     */
    public function readString()
    {
        return $this->readBytes();
    }

    /**
     * @return string
     */
    public function readBytes()
    {
        return $this->read($this->readLong());
    }

    /**
     * @param int $len count of bytes to read
     *
     * @return string
     */
    public function read($len)
    {
        return $this->io->read($len);
    }

    /**
     * @return mixed|null
     */
    public function skipNull()
    {
        return null;
    }

    /**
     * @return bool|mixed
     */
    public function skipBoolean()
    {
        return $this->skip(1);
    }

    /**
     * @return mixed|void
     */
    public function skipInt()
    {
        $this->skipLong();
    }

    /**
     * @return bool|mixed
     */
    public function skipFloat()
    {
        return $this->skip(4);
    }

    /**
     * @return bool|mixed
     */
    public function skipDouble()
    {
        return $this->skip(8);
    }

    /**
     * @return bool|mixed
     */
    public function skipBytes()
    {
        return $this->skip($this->readLong());
    }

    /**
     * @return bool|mixed
     */
    public function skipString()
    {
        return $this->skipBytes();
    }

    public function skipArray()
    {
// todo
    }

 /*
  *
  * array map union enum fixed record
def skip_fixed(writers_schema, decoder)
decoder.skip(writers_schema.size)
end

def skip_enum(writers_schema, decoder)
decoder.skip_int
end

def skip_union(writers_schema, decoder)
index = decoder.read_long
skip_data(writers_schema.schemas[index], decoder)
end

def skip_array(writers_schema, decoder)
skip_blocks(decoder) { skip_data(writers_schema.items, decoder) }
end

      def skip_map(writers_schema, decoder)
        skip_blocks(decoder) {
        decoder.skip_string
          skip_data(writers_schema.values, decoder)
        }
      end

      def skip_record(writers_schema, decoder)
        writers_schema.fields.each{|f| skip_data(f.type, decoder) }
      end
*/

    /**
     * @param int $len Count of bytes to skip
     *
     * @return bool
     * @uses IOInterface::seek()
     */
    public function skip($len)
    {
        return $this->seek($len, IOInterface::SEEK_CUR);
    }
}
