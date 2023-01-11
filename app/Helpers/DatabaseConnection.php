<?php

namespace App\Helpers;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Config;
use Illuminate\Support\Facades\Schema;
use PDO;

class DatabaseConnection
{
    public static function setConnection($driver, $connection_name, $id)
    {
        $connection = DB::connection('mysql2')->table('CT_CONNECTIONS')->find($id);
        if (strtolower($driver) == "oracle") {
            config(['database.connections.' . $connection_name . '' => [
                'driver' => strtolower($driver),
                'tns'            => env('DB_TNS', ''),
                'host' => $connection->connection_host,
                'port' => $connection->connection_port,
                'database' => $connection->database_name,
                'service_name'   => env('DB_SERVICE_NAME', ''),
                'username' => $connection->connection_username,
                'password' => $connection->connection_password,
                'charset'        => env('DB_CHARSET', 'AL32UTF8'),
                'prefix'         => env('DB_PREFIX', ''),
                'prefix_schema'  => env('DB_SCHEMA_PREFIX', ''),
                'edition'        => env('DB_EDITION', 'ora$base'),
                'server_version' => env('DB_SERVER_VERSION', '11g'),
                'load_balance'   => env('DB_LOAD_BALANCE', 'yes'),
                'dynamic'        => [],

            ]]);
        } else {
            config(['database.connections.' . $connection_name . '' => [
                'driver' => strtolower($driver),
                'url' => env('DATABASE_URL_' . $connection_name . ''),
                'host' => $connection->connection_host,
                'port' => $connection->connection_port,
                'database' => $connection->database_name,
                'username' => $connection->connection_username,
                'password' => $connection->connection_password,
                'unix_socket' => env('DB_SOCKET_' . $connection_name . '', ''),
                'charset' => 'utf8mb4',
                'collation' => 'utf8mb4_unicode_ci',
                'prefix' => '',
                'prefix_indexes' => true,
                'strict' => true,
                'engine' => null,
                'options' => extension_loaded('pdo_mysql') ? array_filter([
                    PDO::MYSQL_ATTR_SSL_CA => env('MYSQL_ATTR_SSL_CA'),
                ]) : [],

            ]]);
        }

        return DB::connection($connection_name);
    }
    public static function getSchema($driver, $connection_name,$id)
    {
        $connection = DB::connection('mysql2')->table('CT_CONNECTIONS')->find($id);
        if (strtolower($driver) == "oracle") {
            config(['database.connections.' . $connection_name . '' => [
                'driver' => strtolower($driver),
                'tns'            => env('DB_TNS', ''),
                'host' => $connection->connection_host,
                'port' => $connection->connection_port,
                'database' => $connection->database_name,
                'service_name'   => env('DB_SERVICE_NAME', ''),
                'username' => $connection->connection_username,
                'password' => $connection->connection_password,
                'charset'        => env('DB_CHARSET', 'AL32UTF8'),
                'prefix'         => env('DB_PREFIX', ''),
                'prefix_schema'  => env('DB_SCHEMA_PREFIX', ''),
                'edition'        => env('DB_EDITION', 'ora$base'),
                'server_version' => env('DB_SERVER_VERSION', '11g'),
                'load_balance'   => env('DB_LOAD_BALANCE', 'yes'),
                'dynamic'        => [],

            ]]);
        } else {
            config(['database.connections.' . $connection_name . '' => [
                'driver' => strtolower($driver),
                'url' => env('DATABASE_URL_' . $connection_name . ''),
                'host' => $connection->connection_host,
                'port' => $connection->connection_port,
                'database' => $connection->database_name,
                'username' => $connection->connection_username,
                'password' => $connection->connection_password,
                'unix_socket' => env('DB_SOCKET_' . $connection_name . '', ''),
                'charset' => 'utf8mb4',
                'collation' => 'utf8mb4_unicode_ci',
                'prefix' => '',
                'prefix_indexes' => true,
                'strict' => true,
                'engine' => null,
                'options' => extension_loaded('pdo_mysql') ? array_filter([
                    PDO::MYSQL_ATTR_SSL_CA => env('MYSQL_ATTR_SSL_CA'),
                ]) : [],

            ]]);
        }
        return Schema::connection($connection_name);
    }
}
