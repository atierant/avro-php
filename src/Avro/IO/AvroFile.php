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

use Avro\Exception\IO\AvroIOException;

/**
 * Class AvroFile
 *
 * AvroIO wrapper for PHP file access functions
 */
class AvroFile extends AbstractAvroIO implements IOInterface
{
    /** @var string fopen read mode value. Used internally. */
    const FOPEN_READ_MODE = 'rb';

    /** @var string fopen write mode value. Used internally. */
    const FOPEN_WRITE_MODE = 'wb';

    /** @var string */
    private $filePath;

    /** @var resource file handle for Avro\IO\AvroFile instance */
    private $fileHandle;

    /**
     * AvroFile constructor.
     *
     * @param string $filePath
     * @param string $mode
     *
     * @throws AvroIOException
     */
    public function __construct($filePath, $mode = self::READ_MODE)
    {
        /**
         * XXX: should we check for file existence (in case of reading)
         * or anything else about the provided file_path argument?
         */
        $this->filePath = $filePath;
        switch ($mode) {
            case self::WRITE_MODE:
                $this->fileHandle = fopen($this->filePath, self::FOPEN_WRITE_MODE);
                if (false == $this->fileHandle) {
                    throw new AvroIOException('Could not open file for writing');
                }
                break;
            case self::READ_MODE:
                $this->fileHandle = fopen($this->filePath, self::FOPEN_READ_MODE);
                if (false == $this->fileHandle) {
                    throw new AvroIOException('Could not open file for reading');
                }
                break;
            default:
                throw new AvroIOException(sprintf(
                    "Only modes '%s' and '%s' allowed. You provided '%s'.",
                    self::READ_MODE,
                    self::WRITE_MODE,
                    $mode
                ));
        }
    }

    /**
     * @param string $str
     *
     * @return int count of bytes written
     * @throws AvroIOException if write failed.
     */
    public function write($str)
    {
        $len = fwrite($this->fileHandle, $str);
        if (false === $len) {
            throw new AvroIOException(sprintf('Could not write to file'));
        }

        return $len;
    }

    /**
     * @param int $len count of bytes to read.
     *
     * @return string bytes read
     * @throws AvroIOException if length value is negative or if the read failed
     */
    public function read($len)
    {
        if (0 > $len) {
            throw new AvroIOException(sprintf("Invalid length value passed to read: %d", $len));
        }

        if (0 == $len) {
            return '';
        }

        $bytes = fread($this->fileHandle, $len);
        if (false === $bytes) {
            throw new AvroIOException('Could not read from file');
        }

        return $bytes;
    }

    /**
     * @return int current position within the file
     * @throws AvroIOException
     */
    public function tell()
    {
        $position = ftell($this->fileHandle);
        if (false === $position) {
            throw new AvroIOException('Could not execute tell on reader');
        }

        return $position;
    }

    /**
     * @param int $offset
     * @param int $whence
     *
     * @return boolean true upon success
     * @throws AvroIOException if seek failed.
     * @see IOInterface::seek()
     */
    public function seek($offset, $whence = SEEK_SET)
    {
        $res = fseek($this->fileHandle, $offset, $whence);
        // Note: does not catch seeking beyond end of file
        if (-1 === $res) {
            throw new AvroIOException(sprintf("Could not execute seek (offset = %d, whence = %d)", $offset, $whence));
        }

        return true;
    }

    /**
     * Closes the file.
     * @return boolean true if successful.
     * @throws AvroIOException if there was an error closing the file.
     */
    public function close()
    {
        $res = fclose($this->fileHandle);
        if (false === $res) {
            throw new AvroIOException('Error closing file.');
        }

        return $res;
    }

    /**
     * @return boolean true if the pointer is at the end of the file, and false otherwise.
     * @throws AvroIOException
     * @see IOInterface::isEof() as behavior differs from feof()
     */
    public function isEof()
    {
        $this->read(1);
        if (feof($this->fileHandle)) {
            return true;
        }
        $this->seek(-1, self::SEEK_CUR);

        return false;
    }

    /**
     * @return boolean true if the flush was successful.
     * @throws AvroIOException if there was an error flushing the file.
     */
    public function flush()
    {
        $res = fflush($this->fileHandle);
        if (false === $res) {
            throw new AvroIOException('Could not flush file.');
        }

        return true;
    }
}
