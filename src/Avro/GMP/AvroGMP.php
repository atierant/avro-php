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

namespace Avro\GMP;

use GMP;

/**
 * Class AvroGMP
 *
 * Methods for handling 64-bit operations using the GMP extension.
 *
 * This is a naive and hackish implementation that is intended
 * to work well enough to support Avro. It has not been tested
 * beyond what's needed to decode and encode long values.
 */
class AvroGMP
{
    /** @var resource memoized GMP resource for zero */
    private static $gmp0;
    /** @var resource memoized GMP resource for one (1) */
    private static $gmp1;
    /**  @var resource memoized GMP resource for two (2) */
    private static $gmp2;
    /** @var resource memoized GMP resource for 0x7f */
    private static $gmp0x7f;
    /**  @var resource memoized GMP resource for 64-bit ~0x7f */
    private static $gmpn0x7f;
    /** @var resource memoized GMP resource for 64-bits of 1 */
    private static $gmp0xfs;

    /**
     * @return resource GMP resource for zero
     */
    private static function gmp0()
    {
        if (!isset(self::$gmp0)) {
            self::$gmp0 = gmp_init('0');
        }

        return self::$gmp0;
    }

    /**
     * @return resource GMP resource for one (1)
     */
    private static function gmp1()
    {
        if (!isset(self::$gmp1)) {
            self::$gmp1 = gmp_init('1');
        }

        return self::$gmp1;
    }

    /**
     * @return resource GMP resource for two (2)
     */
    private static function gmp2()
    {
        if (!isset(self::$gmp2)) {
            self::$gmp2 = gmp_init('2');
        }

        return self::$gmp2;
    }

    /**
     * @return resource GMP resource for 0x7f
     */
    private static function gmp0x7f()
    {
        if (!isset(self::$gmp0x7f)) {
            self::$gmp0x7f = gmp_init('0x7f');
        }

        return self::$gmp0x7f;
    }

    /**
     * @return resource GMP resource for 64-bit ~0x7f
     */
    private static function gmpn0x7f()
    {
        if (!isset(self::$gmpn0x7f)) {
            self::$gmpn0x7f = gmp_init('0xffffffffffffff80');
        }

        return self::$gmpn0x7f;
    }

    /**
     * @return resource GMP resource for 64-bits of 1
     */
    private static function gmp0xfs()
    {
        if (!isset(self::$gmp0xfs)) {
            self::$gmp0xfs = gmp_init('0xffffffffffffffff');
        }

        return self::$gmp0xfs;
    }

    /**
     * @param resource|int|string GMP resource
     *
     * @return GMP resource 64-bit two's complement of input.
     */
    public static function gmpTwosComplement($g)
    {
        return gmp_neg(gmp_sub(gmp_pow(self::gmp2(), 64), $g));
    }

    /**
     * @interal Only works up to shift 63 (doesn't wrap bits around).
     *
     * @param resource|int|string $g
     * @param int                 $shift number of bits to shift left
     *
     * @return resource $g shifted left
     */
    public static function shiftLeft($g, $shift)
    {
        if (0 == $shift) {
            return $g;
        }

        if (0 > gmp_sign($g)) {
            $g = self::gmpTwosComplement($g);
        }

        $m = gmp_mul($g, gmp_pow(self::gmp2(), $shift));
        $m = gmp_and($m, self::gmp0xfs());
        if (gmp_testbit($m, 63)) {
            $m = gmp_neg(gmp_add(gmp_and(gmp_com($m), self::gmp0xfs()), self::gmp1()));
        }

        return $m;
    }

    /**
     * Arithmetic right shift
     *
     * @param resource|int|string $g
     * @param int                 $shift number of bits to shift right
     *
     * @return resource $g shifted right $shift bits
     */
    public static function shiftRight($g, $shift)
    {
        if (0 == $shift) {
            return $g;
        }

        if (0 <= gmp_sign($g)) {
            $m = gmp_div($g, gmp_pow(self::gmp2(), $shift));
        } else { // negative
            $g = gmp_and($g, self::gmp0xfs());
            $m = gmp_div($g, gmp_pow(self::gmp2(), $shift));
            $m = gmp_and($m, self::gmp0xfs());
            for ($i = 63; $i >= (63 - $shift); $i--) {
                gmp_setbit($m, $i);
            }

            $m = gmp_neg(gmp_add(gmp_and(gmp_com($m), self::gmp0xfs()), self::gmp1()));
        }

        return $m;
    }

    /**
     * @param int|string $n integer (or string representation of integer) to encode
     *
     * @return string $bytes of the long $n encoded per the Avro spec
     */
    public static function encodeLong($n)
    {
        $g     = gmp_init($n);
        $g     = gmp_xor(self::shiftLeft($g, 1), self::shiftRight($g, 63));
        $bytes = '';
        while (0 != gmp_cmp(self::gmp0(), gmp_and($g, self::gmpn0x7f()))) {
            $bytes .= chr(gmp_intval(gmp_and($g, self::gmp0x7f())) | 0x80);
            $g     = self::shiftRight($g, 7);
        }
        $bytes .= chr(gmp_intval($g));

        return $bytes;
    }

    /**
     * @param int[] $bytes array of ascii codes of bytes to decode
     *
     * @return string represenation of decoded long.
     */
    public static function decodeLongFromArray($bytes)
    {
        $b     = array_shift($bytes);
        $g     = gmp_init($b & 0x7f);
        $shift = 7;
        while (0 != ($b & 0x80)) {
            $b     = array_shift($bytes);
            $g     = gmp_or($g, self::shiftLeft(($b & 0x7f), $shift));
            $shift += 7;
        }
        $val = gmp_xor(self::shiftRight($g, 1), gmp_neg(gmp_and($g, 1)));

        return gmp_strval($val);
    }
}
