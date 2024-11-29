<?php

namespace App\Http\Controllers\Examples;

use App\Http\Controllers\Controller;
use ClickHouseDB\Client;
use Illuminate\Http\Request;

class JsonController extends Controller
{

    //================================= IMPORTANT========================================
    // CLICKHOUSE now workig under json table and existing json type under proccess, docs does not suggest to use yet it
    // instead of this approach adviced working with normal tables
    /*
    ------EX:----
                {
                      "date": "2022-11-15",
                      "country_code": "ES",
                      "project": "clickhouse-connect",
                      "type": "bdist_wheel",
                      "installer": "pip",
                      "python_minor": "3.9",
                      "system": "Linux",
                      "version": "0.3.0"
                }

                CREATE TABLE pypi (
                    `date` Date,
                    `country_code` String,
                    `project` String,
                    `type` String,
                    `installer` String,
                    `python_minor` String,
                    `system` String,
                    `version` String
                )
                ENGINE = MergeTree
                ORDER BY (project, date)





                =====or =====


                another way storing whole data as string
                CREATE TABLE arxiv (
                  body String
                )
                ENGINE = MergeTree ORDER BY ()



                INSERT INTO arxiv SELECT *
                FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/arxiv/arxiv.json.gz', 'JSONAsString')
                ===========================================

                better way is
            {
                "id": 1,
                "name": "Clicky McCliickHouse",
                "username": "Clicky",
                "email": "clicky@clickhouse.com",
                "address": [
                  {
                    "street": "Victor Plains",
                    "city": "Wisokyburgh",
                    "zipcode": "90566-7771"
                  }
                ],
                "website": "clickhouse.com"
            }

    //
    */








    protected $clickhouse;
    public function __construct(Client $clickhouse)
    {
        $this->clickhouse = $clickhouse;
    }


    //
    public function createPeopleTable()
    {
        $sql = <<<SQL
                CREATE TABLE people
                        (
                            `id` Int64,
                            `name` String,
                            `username` String,
                            `email` String,
                            `address` Array(Tuple(city String, geo Tuple(lat Float32, lng Float32), street String, suite String, zipcode String)),
                            `phone_numbers` Array(String),
                            `website` String,
                            `company` Tuple(catchPhrase String, name String, labels Map(String,String)),
                            `dob` Date,
                            `tags` String
                        )
                        ENGINE = MergeTree
                ORDER BY username
            SQL;


        try {
            $this->clickhouse->write($sql);
            return response()->json(['status' => 'people table created succesfully']);
        } catch (\Exception $e) {
            return response()->json(['error' => 'error message: ' . $e]);
        }
    }

    // populate people table with data incuding array
    public function insertDataToPeople()
    {
        $sql = <<<SQL
            INSERT INTO people FORMAT JSONEachRow
            {"id":1,"name":"Clicky McCliickHouse","username":"Clicky","email":"clicky@clickhouse.com","address":[{"street":"Victor Plains","suite":"Suite 879","city":"Wisokyburgh","zipcode":"90566-7771","geo":{"lat":-43.9509,"lng":-34.4618}}],"phone_numbers":["010-692-6593","020-192-3333"],"website":"clickhouse.com","company":{"name":"ClickHouse","catchPhrase":"The real-time data warehouse for analytics","labels":{"type":"database systems","founded":"2021"}},"dob":"2007-03-31","tags":{"hobby":"Databases","holidays":[{"year":2024,"location":"Azores, Portugal"}],"car":{"model":"Tesla","year":2023}}}
        SQL;
        try {
            $this->clickhouse->write($sql);
            return response()->json(['status' => 'success']);
        } catch (\Exception $e) {
            return response()->json(['error' => 'error: ' . $e]);
        }
    }



    // query get from arxiv table
    public function queryJson($text)
    {
        // Escape user input to prevent SQL injection
        $escapedText = addslashes($text);

        // Use heredoc for SQL query
        $sql = <<<SQL
            SELECT count(*)
            FROM arxiv
            WHERE JSONExtractString(body, 'abstract') LIKE '%$text%' LIMIT 1;
        SQL;

        return $this->clickhouse->select($sql)->rows();
    }
}
