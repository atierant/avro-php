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

use Avro\Debug\AvroDebug;
use PHPUnit\Framework\TestCase;

/**
 * Class FloatIntEncodingTest
 */
class FloatIntEncodingTest extends TestCase
{
    const FLOAT_TYPE  = 'float';
    const DOUBLE_TYPE = 'double';

    public static $floatNan;
    public static $floatPosInf;
    public static $floatNegInf;
    public static $doubleNan;
    public static $doublePosInf;
    public static $doubleNegInf;

    public static $longBitsNan;
    public static $longBitsPosInf;
    public static $longBitsNegInf;
    public static $intBitsNan;
    public static $intBitsPosInf;
    public static $intBitsNegInf;

    public static function makeSpecialVals()
    {
        self::$doubleNan    = (double) NAN;
        self::$doublePosInf = (double) INF;
        self::$doubleNegInf = (double) -INF;
        self::$floatNan     = (float) NAN;
        self::$floatPosInf  = (float) INF;
        self::$floatNegInf  = (float) -INF;

        self::$longBitsNan    = strrev(pack('H*', '7ff8000000000000'));
        self::$longBitsPosInf = strrev(pack('H*', '7ff0000000000000'));
        self::$longBitsNegInf = strrev(pack('H*', 'fff0000000000000'));
        self::$intBitsNan     = strrev(pack('H*', '7fc00000'));
        self::$intBitsPosInf  = strrev(pack('H*', '7f800000'));
        self::$intBitsNegInf  = strrev(pack('H*', 'ff800000'));
    }

    public function setUp()
    {
        self::makeSpecialVals();
    }

    public function testSpecialValues()
    {
        self::assertTrue(is_float(self::$floatNan), 'float NaN is a float');
        self::assertTrue(is_nan(self::$floatNan), 'float NaN is NaN');
        self::assertFalse(is_infinite(self::$floatNan), 'float NaN is not infinite');

        self::assertTrue(is_float(self::$floatPosInf), 'float pos infinity is a float');
        self::assertTrue(is_infinite(self::$floatPosInf), 'float pos infinity is infinite');
        self::assertTrue(0 < self::$floatPosInf, 'float pos infinity is greater than 0');
        self::assertFalse(is_nan(self::$floatPosInf), 'float pos infinity is not NaN');

        self::assertTrue(is_float(self::$floatNegInf), 'float neg infinity is a float');
        self::assertTrue(is_infinite(self::$floatNegInf), 'float neg infinity is infinite');
        self::assertTrue(0 > self::$floatNegInf, 'float neg infinity is less than 0');
        self::assertFalse(is_nan(self::$floatNegInf), 'float neg infinity is not NaN');

        self::assertTrue(is_double(self::$doubleNan), 'double NaN is a double');
        self::assertTrue(is_nan(self::$doubleNan), 'double NaN is NaN');
        self::assertFalse(is_infinite(self::$doubleNan), 'double NaN is not infinite');

        self::assertTrue(is_double(self::$doublePosInf), 'double pos infinity is a double');
        self::assertTrue(is_infinite(self::$doublePosInf), 'double pos infinity is infinite');
        self::assertTrue(0 < self::$doublePosInf, 'double pos infinity is greater than 0');
        self::assertFalse(is_nan(self::$doublePosInf), 'double pos infinity is not NaN');

        self::assertTrue(is_double(self::$doubleNegInf), 'double neg infinity is a double');
        self::assertTrue(is_infinite(self::$doubleNegInf), 'double neg infinity is infinite');
        self::assertTrue(0 > self::$doubleNegInf, 'double neg infinity is less than 0');
        self::assertFalse(is_nan(self::$doubleNegInf), 'double neg infinity is not NaN');
    }

    /**
     * @return array
     */
    public function specialValsProvider()
    {
        self::makeSpecialVals();

        return [
            [self::DOUBLE_TYPE, self::$doublePosInf, self::$longBitsPosInf],
            [self::DOUBLE_TYPE, self::$doubleNegInf, self::$longBitsNegInf],
            [self::FLOAT_TYPE, self::$floatPosInf, self::$intBitsPosInf],
            [self::FLOAT_TYPE, self::$floatNegInf, self::$intBitsNegInf],
        ];
    }

    /**
     * @dataProvider specialValsProvider
     *
     * @param $type
     * @param $val
     * @param $bits
     */
    public function testEncodingSpecialValues($type, $val, $bits)
    {
        $this->assertEncodeValues($type, $val, $bits);
    }

    /**
     * @return array
     */
    public function nanValsProvider()
    {
        self::makeSpecialVals();

        return [
            [self::DOUBLE_TYPE, self::$doubleNan, self::$longBitsNan],
            [self::FLOAT_TYPE, self::$floatNan, self::$intBitsNan],
        ];
    }

    /**
     * @dataProvider nanValsProvider
     *
     * @param $type
     * @param $val
     * @param $bits
     */
    public function testEncodingNanValues($type, $val, $bits)
    {
        $this->assertEncodeNanValues($type, $val, $bits);
    }

    /**
     * @return array
     */
    public function normalValsProvider()
    {
        return [
            [self::DOUBLE_TYPE, (double) -10, "\000\000\000\000\000\000$\300", '000000000000420c'],
            [self::DOUBLE_TYPE, (double) -9, "\000\000\000\000\000\000\"\300", '000000000000220c'],
            [self::DOUBLE_TYPE, (double) -8, "\000\000\000\000\000\000 \300", '000000000000020c'],
            [self::DOUBLE_TYPE, (double) -7, "\000\000\000\000\000\000\034\300", '000000000000c10c'],
            [self::DOUBLE_TYPE, (double) -6, "\000\000\000\000\000\000\030\300", '000000000000810c'],
            [self::DOUBLE_TYPE, (double) -5, "\000\000\000\000\000\000\024\300", '000000000000410c'],
            [self::DOUBLE_TYPE, (double) -4, "\000\000\000\000\000\000\020\300", '000000000000010c'],
            /**/
            [self::DOUBLE_TYPE, (double) -3, "\000\000\000\000\000\000\010\300", '000000000000800c'],
            [self::DOUBLE_TYPE, (double) -2, "\000\000\000\000\000\000\000\300", '000000000000000c'],
            [self::DOUBLE_TYPE, (double) -1, "\000\000\000\000\000\000\360\277", '0000000000000ffb'],
            [self::DOUBLE_TYPE, (double) 0, "\000\000\000\000\000\000\000\000", '0000000000000000'],
            [self::DOUBLE_TYPE, (double) 1, "\000\000\000\000\000\000\360?", '0000000000000ff3'],
            [self::DOUBLE_TYPE, (double) 2, "\000\000\000\000\000\000\000@", '0000000000000004'],
            /**/
            [self::DOUBLE_TYPE, (double) 3, "\000\000\000\000\000\000\010@", '0000000000008004'],
            [self::DOUBLE_TYPE, (double) 4, "\000\000\000\000\000\000\020@", '0000000000000104'],
            [self::DOUBLE_TYPE, (double) 5, "\000\000\000\000\000\000\024@", '0000000000004104'],
            [self::DOUBLE_TYPE, (double) 6, "\000\000\000\000\000\000\030@", '0000000000008104'],
            [self::DOUBLE_TYPE, (double) 7, "\000\000\000\000\000\000\034@", '000000000000c104'],
            [self::DOUBLE_TYPE, (double) 8, "\000\000\000\000\000\000 @", '0000000000000204'],
            [self::DOUBLE_TYPE, (double) 9, "\000\000\000\000\000\000\"@", '0000000000002204'],
            [self::DOUBLE_TYPE, (double) 10, "\000\000\000\000\000\000$@", '0000000000004204'],
            /**/
            [self::DOUBLE_TYPE, (double) -1234.2132, "\007\316\031Q\332H\223\300", '70ec9115ad84390c'],
            [self::DOUBLE_TYPE, (double) -2.11e+25, "\311\260\276J\031t1\305", '9c0beba49147135c'],

            [self::FLOAT_TYPE, (float) -10, "\000\000 \301", '0000021c'],
            [self::FLOAT_TYPE, (float) -9, "\000\000\020\301", '0000011c'],
            [self::FLOAT_TYPE, (float) -8, "\000\000\000\301", '0000001c'],
            [self::FLOAT_TYPE, (float) -7, "\000\000\340\300", '00000e0c'],
            [self::FLOAT_TYPE, (float) -6, "\000\000\300\300", '00000c0c'],
            [self::FLOAT_TYPE, (float) -5, "\000\000\240\300", '00000a0c'],
            [self::FLOAT_TYPE, (float) -4, "\000\000\200\300", '0000080c'],
            [self::FLOAT_TYPE, (float) -3, "\000\000@\300", '0000040c'],
            [self::FLOAT_TYPE, (float) -2, "\000\000\000\300", '0000000c'],
            [self::FLOAT_TYPE, (float) -1, "\000\000\200\277", '000008fb'],
            [self::FLOAT_TYPE, (float) 0, "\000\000\000\000", '00000000'],
            [self::FLOAT_TYPE, (float) 1, "\000\000\200?", '000008f3'],
            [self::FLOAT_TYPE, (float) 2, "\000\000\000@", '00000004'],
            [self::FLOAT_TYPE, (float) 3, "\000\000@@", '00000404'],
            [self::FLOAT_TYPE, (float) 4, "\000\000\200@", '00000804'],
            [self::FLOAT_TYPE, (float) 5, "\000\000\240@", '00000a04'],
            [self::FLOAT_TYPE, (float) 6, "\000\000\300@", '00000c04'],
            [self::FLOAT_TYPE, (float) 7, "\000\000\340@", '00000e04'],
            [self::FLOAT_TYPE, (float) 8, "\000\000\000A", '00000014'],
            [self::FLOAT_TYPE, (float) 9, "\000\000\020A", '00000114'],
            [self::FLOAT_TYPE, (float) 10, "\000\000 A", '00000214'],
            [self::FLOAT_TYPE, (float) -1234.5, "\000P\232\304", '0005a94c'],
            [self::FLOAT_TYPE, (float) -211300000.0, "\352\202I\315", 'ae2894dc'],
        ];
    }

    /**
     * @return array
     */
    public function floatValsProvider()
    {
        $ary = [];

        foreach ($this->normalValsProvider() as $values) {
            if (self::FLOAT_TYPE == $values[0]) {
                $ary [] = [$values[0], $values[1], $values[2]];
            }
        }

        return $ary;
    }

    /**
     * @return array
     */
    public function doubleValsProvider()
    {
        $ary = [];

        foreach ($this->normalValsProvider() as $values) {
            if (self::DOUBLE_TYPE == $values[0]) {
                $ary [] = [$values[0], $values[1], $values[2]];
            }
        }

        return $ary;
    }


    /**
     * @dataProvider floatValsProvider
     *
     * @param $type
     * @param $val
     * @param $bits
     */
    public function testEncodingFloatValues($type, $val, $bits)
    {
        $this->assertEncodeValues($type, $val, $bits);
    }

    /**
     * @dataProvider doubleValsProvider
     *
     * @param $type
     * @param $val
     * @param $bits
     */
    public function testEncodingDoubleValues($type, $val, $bits)
    {
        $this->assertEncodeValues($type, $val, $bits);
    }

    /**
     * @param $type
     * @param $val
     * @param $bits
     */
    public function assertEncodeValues($type, $val, $bits)
    {
        if (self::FLOAT_TYPE == $type) {
            $decoder = ['Avro\Datum\AvroIOBinaryDecoder', 'intBitsToFloat'];
            $encoder = ['Avro\Datum\AvroIOBinaryEncoder', 'floatToIntBits'];
        } else {
            $decoder = ['Avro\Datum\AvroIOBinaryDecoder', 'longBitsToDouble'];
            $encoder = ['Avro\Datum\AvroIOBinaryEncoder', 'doubleToLongBits'];
        }

        $decodedBitsVal = call_user_func($decoder, $bits);
        self::assertEquals($val, $decodedBitsVal, sprintf(
            "%s\n expected: '%f'\n    given: '%f'",
            'DECODED BITS',
            $val,
            $decodedBitsVal
        ));

        $encodedValBits = call_user_func($encoder, $val);
        self::assertEquals($bits, $encodedValBits, sprintf(
            "%s\n expected: '%s'\n    given: '%s'",
            'ENCODED VAL',
            AvroDebug::hexString($bits),
            AvroDebug::hexString($encodedValBits)
        ));

        $roundTripValue = call_user_func($decoder, $encodedValBits);
        self::assertEquals($val, $roundTripValue, sprintf(
            "%s\n expected: '%f'\n     given: '%f'",
            'ROUND TRIP BITS',
            $val,
            $roundTripValue
        ));
    }

    /**
     * @param $type
     * @param $val
     * @param $bits
     */
    public function assertEncodeNanValues($type, $val, $bits)
    {
        if (self::FLOAT_TYPE == $type) {
            $decoder = ['Avro\Datum\AvroIOBinaryDecoder', 'intBitsToFloat'];
            $encoder = ['Avro\Datum\AvroIOBinaryEncoder', 'floatToIntBits'];
        } else {
            $decoder = ['Avro\Datum\AvroIOBinaryDecoder', 'longBitsToDouble'];
            $encoder = ['Avro\Datum\AvroIOBinaryEncoder', 'doubleToLongBits'];
        }

        $decodedBitsVal = call_user_func($decoder, $bits);
        self::assertTrue(is_nan($decodedBitsVal), sprintf(
            "%s\n expected: '%f'\n    given: '%f'",
            'DECODED BITS',
            $val,
            $decodedBitsVal
        ));

        $encodedValBits = call_user_func($encoder, $val);
        self::assertEquals($bits, $encodedValBits, sprintf(
            "%s\n expected: '%s'\n    given: '%s'",
            'ENCODED VAL',
            AvroDebug::hexString($bits),
            AvroDebug::hexString($encodedValBits)
        ));

        $roundTripValue = call_user_func($decoder, $encodedValBits);
        self::assertTrue(is_nan($roundTripValue), sprintf(
            "%s\n expected: '%f'\n     given: '%f'",
            'ROUND TRIP BITS',
            $val,
            $roundTripValue
        ));
    }
}
