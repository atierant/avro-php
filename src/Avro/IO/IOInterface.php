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

/**
 * Interface IOInterface
 *
 * Barebones IO base class to provide common interface for file and string access within the classes.
 */
interface IOInterface
{
    /** @var string general read mode */
    const READ_MODE = 'r';

    /** @var string general write mode. */
    const WRITE_MODE = 'w';

    /** @var int set position equal to $offset bytes */
    const SEEK_CUR = SEEK_CUR;

    /** @var int set position to current index + $offset bytes */
    const SEEK_SET = SEEK_SET;

    /** @var int set position to end of file + $offset bytes */
    const SEEK_END = SEEK_END;

    /**
     * Read $len bytes from IO instance
     *
     * @var int $len
     * @return string bytes read
     */
    public function read($len);

    /**
     * Append bytes to this buffer.
     *
     * @param string $arg bytes to write
     *
     * @return int count of bytes written.
     */
    public function write($arg);

    /**
     * Return byte offset within IO instance
     *
     * @return int
     */
    public function tell();

    /**
     * Set the position indicator. The new position, measured in bytes from the beginning of the file,
     * is obtained by adding $offset to the position specified by $whence.
     *
     * @param int $offset
     * @param int $whence one of IOInterface::SEEK_SET, IOInterface::SEEK_CUR, or IOInterface::SEEK_END
     *
     * @return bool true
     */
    public function seek($offset, $whence = self::SEEK_SET);

    /**
     * Flushes any buffered Data to the IO object.
     *
     * @return bool true upon success.
     */
    public function flush();

    /**
     * Returns whether or not the current position at the end of this IO instance.
     *
     * Note isEof() is <b>not</b> like eof in C or feof in PHP:
     * it returns TRUE if the *next* read would be end of file, rather than if the *most recent* read read end of file.
     *
     * @return bool true if at the end of file, and false otherwise
     */
    public function isEof();

    /**
     * Closes this IO instance.
     */
    public function close();
}
