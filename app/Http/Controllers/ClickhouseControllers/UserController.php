<?php

namespace App\Http\Controllers\ClickhouseControllers;

use App\Http\Controllers\Controller;
use ClickHouseDB\Client;

class UserController extends Controller
{
    protected $clickhouse;
    public function __construct(Client $clickhouse)
    {
        $this->clickhouse = $clickhouse;
    }

    //  creat table method
    public function createUsersTable()
    {
        $createQuery = '
        CREATE TABLE users (
            id UInt64,
            name String,
            email String,
            password String,
            created_at DateTime DEFAULT now(),
            updated_at DateTime DEFAULT now(),
            version UInt64
        ) ENGINE = ReplacingMergeTree(version)
        ORDER BY id;
    ';

        try {
            $this->clickhouse->write($createQuery);
            return 'Table created successfully';
        } catch (\Exception $e) {
            return 'Error creating table: ' . $e->getMessage();
        }
    }
}
