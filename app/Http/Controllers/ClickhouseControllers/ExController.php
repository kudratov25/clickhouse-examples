<?php

namespace App\Http\Controllers\ClickhouseControllers;

use App\Http\Controllers\Controller;
use ClickHouseDB\Client;
use Illuminate\Http\Request;

class ExController extends Controller
{
    protected $clickhouse;
    public function __construct(Client $clickhouse)
    {
        $this->clickhouse = $clickhouse;
    }

    // get table info
    public function info($table)
    {
        $tableInfo = $this->clickhouse->showCreateTable($table);
        return $tableInfo;
    }


    //destroy table
    public function destroyTable($table)
    {
        $query = 'DROP TABLE IF EXISTS '. $table;
        try {
            $this->clickhouse->write($query);
            return 'Table truncated successfuly';
        } catch (\Exception $e) {
            return 'Error truncating table: ' . $e->getMessage();
        }
    }
}
