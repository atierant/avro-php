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

namespace Avro\IO;

use Avro\Exception\AvroNotImplementedException;

/**
 * Class AbstractAvroIO
 *
 * Barebones IO base class to provide common interface for file and string access within the Avro classes.
 */
abstract class AbstractAvroIO implements IOInterface
{
    /**
     * Read $len bytes from AvroIO instance
     *
     * @var int $len
     * @return string bytes read
     * @throws AvroNotImplementedException
     */
    public function read($len)
    {
        throw new AvroNotImplementedException('Not implemented');
    }

    /**
     * Append bytes to this buffer. (Nothing more is needed to support Avro.)
     *
     * @param string $arg bytes to write
     *
     * @throws AvroNotImplementedException
     */
    public function write($arg)
    {
        throw new AvroNotImplementedException('Not implemented');
    }

    /**
     * Return byte offset within AvroIO instance
     *
     * @throws AvroNotImplementedException
     */
    public function tell()
    {
        throw new AvroNotImplementedException('Not implemented');
    }

    /**
     * Set the position indicator. The new position, measured in bytes from the beginning of the file,
     * is obtained by adding $offset to the position specified by $whence.
     *
     * @param int $offset
     * @param int $whence one of self::SEEK_SET, self::SEEK_CUR, or self::SEEK_END
     *
     * @throws AvroNotImplementedException
     */
    public function seek($offset, $whence = self::SEEK_SET)
    {
        throw new AvroNotImplementedException('Not implemented');
    }

    /**
     * Flushes any buffered Data to the AvroIO object.
     *
     * @throws AvroNotImplementedException
     */
    public function flush()
    {
        throw new AvroNotImplementedException('Not implemented');
    }

    /**
     * Returns whether or not the current position at the end of this AvroIO instance.
     *
     * Note isEof() is <b>not</b> like eof in C or feof in PHP:
     * it returns TRUE if the *next* read would be end of file, rather than if the *most recent* read read end of file.
     *
     * @throws AvroNotImplementedException
     */
    public function isEof()
    {
        throw new AvroNotImplementedException('Not implemented');
    }

    /**
     * Closes this AvroIO instance.
     *
     * @throws AvroNotImplementedException
     */
    public function close()
    {
        throw new AvroNotImplementedException('Not implemented');
    }
}
