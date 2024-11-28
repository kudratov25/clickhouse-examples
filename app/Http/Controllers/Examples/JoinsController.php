<?php

namespace App\Http\Controllers\Examples;

use App\Http\Controllers\Controller;
use ClickHouseDB\Client;

class JoinsController extends Controller
{
    protected $clickhouse;
    public function __construct(Client $clickhouse)
    {
        $this->clickhouse = $clickhouse;
    }

    // create new cities table
    public function createCitiesTable()
    {
        $sql = <<<SQL
            CREATE TABLE new_cities
            (
                city_id UInt32,
                city_name String,
                created_at DateTime,
                updated_at DateTime,
            )
            ENGINE =ReplacingMergeTree
            PARTITION BY toYYYYMM (created_at)
            ORDER BY city_name;
        SQL;
        try {
            $this->clickhouse->write($sql);
            return response()->json(['status' => 'created successfully new_cities table']);
        } catch (\Exception $e) {
            return response()->json(['error' => 'Error message: ' . $e->getMessage()]);
        }
    }



    // trips table for the joining tables
    public function createTripsTable()
    {
        $sql = <<<SQL
        CREATE TABLE new_trips
                (
                 `trip_id` UInt32,
                 `vendor_id` Enum8('1' = 1, '2' = 2, '3' = 3, '4' = 4, 'CMT' = 5, 'VTS' = 6, 'DDS' = 7, 'B02512' = 10, 'B02598' = 11, 'B02617' = 12, 'B02682' = 13, 'B02764' = 14, '' = 15),
                 `pickup_date` Date,
                 `pickup_datetime` DateTime,
                 `dropoff_date` Date,
                 `dropoff_datetime` DateTime,
                 `store_and_fwd_flag` UInt8,
                 `rate_code_id` UInt8,
                 `pickup_longitude` Float64,
                 `pickup_latitude` Float64,
                 `dropoff_longitude` Float64,
                 `dropoff_latitude` Float64,
                 `passenger_count` UInt8,
                 `trip_distance` Float64,
                 `fare_amount` Float32,
                 `extra` Float32,
                 `mta_tax` Float32,
                 `tip_amount` Float32,
                 `tolls_amount` Float32,
                 `ehail_fee` Float32,
                 `improvement_surcharge` Float32,
                 `total_amount` Float32,
                 `payment_type` Enum8('UNK' = 0, 'CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4),
                 `trip_type` UInt8,
                 `pickup` FixedString(25),
                 `dropoff` FixedString(25),
                 `cab_type` Enum8('yellow' = 1, 'green' = 2, 'uber' = 3),
                 `pickup_nyct2010_gid` Int8,
                 `pickup_ctlabel` Float32,
                 `pickup_borocode` Int8,
                 `pickup_ct2010` String,
                 `pickup_boroct2010` String,
                 `pickup_cdeligibil` String,
                 `pickup_ntacode` FixedString(4),
                 `city_id` UInt16,
                 `pickup_puma` UInt16,
                 `dropoff_nyct2010_gid` UInt8,
                 `dropoff_ctlabel` Float32,
                 `dropoff_borocode` UInt8,
                 `dropoff_ct2010` String,
                 `dropoff_boroct2010` String,
                 `dropoff_cdeligibil` String,
                 `dropoff_ntacode` FixedString(4),
                 `dropoff_ntaname` String,
                 `dropoff_puma` UInt16
                )
                ENGINE = MergeTree
                PARTITION BY toYYYYMM(pickup_date)
                ORDER BY pickup_datetime;
        SQL;
        try {
            $this->clickhouse->write($sql);
            return response()->json(['status' => 'Successfully created table']);
        } catch (\Exception $e) {
            return response()->json(['error' => 'Error message: ' . $e->getMessage()]);
        }
    }

    // seed cities table from amazon dataset get unique names and save it cities "table"
    public function seedCitiesTable()
    {
        $sql = <<<SQL
                INSERT INTO new_cities (city_name)
                SELECT DISTINCT pickup_ntaname
                FROM s3(
                'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_{1..2}.gz',
                'TabSeparatedWithNames', "
                `pickup_ntaname` String"
                );
        SQL;
        try {
            $this->clickhouse->write($sql);
            return response()->json(['status' => 'imported']);
        } catch (\Exception $e) {
            return response()->json(['error' => 'Error message: ' . $e->getMessage()]);
        }
    }



    // trips tables seeded with city_id where table column name city_id mathches to the pickup_ntaname
    public function seedNewTripsTable()
    {
        $sql = "
            INSERT INTO new_trips
                SELECT
                    trip_id,
                    vendor_id,
                    pickup_date,
                    pickup_datetime,
                    dropoff_date,
                    dropoff_datetime,
                    store_and_fwd_flag,
                    rate_code_id,
                    pickup_longitude,
                    pickup_latitude,
                    dropoff_longitude,
                    dropoff_latitude,
                    passenger_count,
                    trip_distance,
                    fare_amount,
                    extra,
                    mta_tax,
                    tip_amount,
                    tolls_amount,
                    ehail_fee,
                    improvement_surcharge,
                    total_amount,
                    payment_type,
                    trip_type,
                    pickup,
                    dropoff,
                    cab_type,
                    pickup_nyct2010_gid,
                    pickup_ctlabel,
                    pickup_borocode,
                    pickup_ct2010,
                    pickup_boroct2010,
                    pickup_cdeligibil,
                    pickup_ntacode,
                    COALESCE(c.city_id, -1) AS city_id, -- Default to -1 if no match is found
                    pickup_puma,
                    dropoff_nyct2010_gid,
                    dropoff_ctlabel,
                    dropoff_borocode,
                    dropoff_ct2010,
                    dropoff_boroct2010,
                    dropoff_cdeligibil,
                    dropoff_ntacode,
                    dropoff_ntaname,
                    dropoff_puma
                FROM s3(
                    'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_{1..2}.gz',
                    'TabSeparatedWithNames',
                    'trip_id UInt32,
                    vendor_id Enum8(\'1\' = 1, \'2\' = 2, \'CMT\' = 5, \'VTS\' = 6, \'DDS\' = 7, \'B02512\' = 10, \'B02598\' = 11, \'B02617\' = 12, \'B02682\' = 13, \'B02764\' = 14, \'\' = 15),
                    pickup_date Date,
                    pickup_datetime DateTime,
                    dropoff_date Date,
                    dropoff_datetime DateTime,
                    store_and_fwd_flag UInt8,
                    rate_code_id UInt8,
                    pickup_longitude Float64,
                    pickup_latitude Float64,
                    dropoff_longitude Float64,
                    dropoff_latitude Float64,
                    passenger_count UInt8,
                    trip_distance Float64,
                    fare_amount Float32,
                    extra Float32,
                    mta_tax Float32,
                    tip_amount Float32,
                    tolls_amount Float32,
                    ehail_fee Float32,
                    improvement_surcharge Float32,
                    total_amount Float32,
                    payment_type Enum8(\'UNK\' = 0, \'CSH\' = 1, \'CRE\' = 2, \'NOC\' = 3, \'DIS\' = 4),
                    trip_type UInt8,
                    pickup FixedString(25),
                    dropoff FixedString(25),
                    cab_type Enum8(\'yellow\' = 1, \'green\' = 2, \'uber\' = 3),
                    pickup_nyct2010_gid Int8,
                    pickup_ctlabel Float32,
                    pickup_borocode Int8,
                    pickup_ct2010 String,
                    pickup_boroct2010 String,
                    pickup_cdeligibil String,
                    pickup_ntacode FixedString(4),
                    pickup_ntaname String,
                    pickup_puma UInt16,
                    dropoff_nyct2010_gid UInt8,
                    dropoff_ctlabel Float32,
                    dropoff_borocode UInt8,
                    dropoff_ct2010 String,
                    dropoff_boroct2010 String,
                    dropoff_cdeligibil String,
                    dropoff_ntacode FixedString(4),
                    dropoff_ntaname String,
                    dropoff_puma UInt16'
                ) AS src
                LEFT JOIN new_cities c
                    ON src.pickup_ntaname = c.city_name -- Match pickup_ntaname with city_name
            SETTINGS input_format_try_infer_datetimes = 0;

        ";

        try {
            $this->clickhouse->write($sql);
            return response()->json(['status' => 'success']);
        } catch (\Exception $e) {
            \Log::error('ClickHouse Query Failed: ' . $sql);
            return response()->json(['error' => 'Error message: ' . $e->getMessage()]);
        }
    }


    public function innerjoin()
    {
        $sql = <<<SQL
            SELECT
                src.trip_id,
                src.pickup_date AS date,
                src.trip_distance AS distance,
                src.pickup_latitude,
                src.pickup_longitude,
                c.city_name
            FROM new_trips AS src
            INNER JOIN new_cities AS c
                ON src.city_id = c.city_id
            LIMIT 100
            SETTINGS input_format_try_infer_datetimes = 0;
        SQL;
        try {
            $data = $this->clickhouse->select($sql)->rows();
            return response()->json(['data' => $data]);
        } catch (\Exception $e) {
            return response()->json(['error' => 'error during data query; ' . $e->getMessage()]);
        }
    }
}
