<?php
/*
 * This file is part of the Avro package.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
date_default_timezone_set('UTC');
require __DIR__ . '/../vendor/autoload.php';
// B.C. for PSR Log's old inheritance
// see https://github.com/php-fig/log/pull/52
if (!class_exists('\\PHPUnit_Framework_TestCase', true)) {
    class_alias('\\PHPUnit\\Framework\\TestCase', '\\PHPUnit_Framework_TestCase');
}

define('AVRO_TEST_HELPER_DIR', dirname(__FILE__));
//require_once(join(DIRECTORY_SEPARATOR, [dirname(AVRO_TEST_HELPER_DIR), 'src', 'Avro', 'Avro.php']));
define('TEST_TEMP_DIR', join(DIRECTORY_SEPARATOR, [AVRO_TEST_HELPER_DIR, 'tmp']));
define('AVRO_BASE_DIR', dirname(dirname(dirname(AVRO_TEST_HELPER_DIR))));
define('AVRO_SHARE_DIR', join(DIRECTORY_SEPARATOR, [AVRO_BASE_DIR, 'share']));
define('AVRO_BUILD_DIR', join(DIRECTORY_SEPARATOR, [AVRO_BASE_DIR, 'build']));
define('AVRO_BUILD_DATA_DIR', join(DIRECTORY_SEPARATOR, [AVRO_BUILD_DIR, 'interop', 'data']));
define('AVRO_TEST_SCHEMAS_DIR', join(DIRECTORY_SEPARATOR, [AVRO_SHARE_DIR, 'test', 'schemas']));
define('AVRO_INTEROP_SCHEMA', join(DIRECTORY_SEPARATOR, [AVRO_TEST_SCHEMAS_DIR, 'interop.avsc']));
