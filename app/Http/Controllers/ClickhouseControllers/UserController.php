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




    // creating indexed optimal users table
    public function createIndexedUsersTable()
    {
        $sql = <<<SQL
            CREATE TABLE indexed_users(
                id UInt64, --unique users ID
                username String, --string username
                email String, --string email
                country String, --string country name
                status UInt8, --status (0,1|deactive,active)
                created_at DateTime, --reg date
                last_login DateTime--last login timestamp
            )
            Engine = MergeTree() --also may use ReplacingMergeTree() but uses high cpu
            PARTITION BY toYYYYMM(created_at) --partition by month
            ORDER BY (country, status, created_at, id) --efficient sorting filter
            SETTINGS index_granularity = 8192;
        SQL;
        try {
            $this->clickhouse->write($sql);
            $checkTable = $this->clickhouse->showCreateTable('indexed_users');
            return response()->json(['status' => 'table created successfully', 'table info' => $checkTable]);
        } catch (\Exception $e) {
            return response()->json(['error' => 'E-Message:' . $e]);
        }
    }

    public function optimizeIndexedUsersTable()
    {
        $sql = <<<SQL
            ALTER TABLE indexed_users ADD INDEX
            email_idx (email) TYPE bloom_filter(0.01) GRANULARITY 4;
        SQL;
        // this is usefull for quick lookup
        try {
            $this->clickhouse->write($sql);
            $checkTable = $this->clickhouse->select('DESCRIBE TABLE indexed_users')->rows();
            return response()->json(['status' => 'successfully applied', 'table info' => $checkTable]);
        } catch (\Exception $e) {
            return response()->json(['error' => 'E-Message:' . $e]);
        }
    }
}
