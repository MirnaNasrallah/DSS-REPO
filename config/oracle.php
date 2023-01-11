<?php

return [
    'oracle' => [
        'driver' => env('DB_CONNECTION_SECOND'),
        'tns'            => env('DB_TNS', ''),
        'host' => env('DB_HOST_SECOND', '127.0.0.1'),
        'port' => env('DB_PORT_SECOND', '1521'),
        'database' => env('DB_DATABASE_SECOND', 'forge'),
        'service_name'   => env('DB_SERVICE_NAME', ''),
        'username' => env('DB_USERNAME_SECOND', 'forge'),
        'password' => env('DB_PASSWORD_SECOND'),
        'charset'        => env('DB_CHARSET', 'AL32UTF8'),
        'prefix'         => env('DB_PREFIX', ''),
        'prefix_schema'  => env('DB_SCHEMA_PREFIX', ''),
        'edition'        => env('DB_EDITION', 'ora$base'),
        'server_version' => env('DB_SERVER_VERSION', '11g'),
        'load_balance'   => env('DB_LOAD_BALANCE', 'yes'),
        'dynamic'        => [],
    ],
];
