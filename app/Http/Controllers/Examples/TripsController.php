<?php

namespace App\Http\Controllers\Examples;

use App\Http\Controllers\Controller;
use ClickHouseDB\Client;
use Illuminate\Http\Request;

class TripsController extends Controller
{
    protected $clickhouse;
    public function __construct(Client $clickhouse)
    {
        $this->clickhouse = $clickhouse;
    }

    public function index()
    {
        $sql = <<<SQL
                SELECT count(*) FROM trips
            SQL;

        try {
            $result = $this->clickhouse->select($sql);
            $content = json_encode($result->rows());
            return response($content, 200);
        } catch (\Exception $e) {
            return response("Error: " . $e->getMessage(), 500);
        }
    }

    // create table trips
    public function createTrips()
    {
        $sql =
            "
            CREATE TABLE trips
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
                     `pickup_ntaname` String,
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
         ";

        try {
            $this->clickhouse->write($sql);
            return 'Trips table succesfully cretaed';
        } catch (\Exception $e) {
            return "Error Cretaing table trips table:" . $e->getMessage();
        }
    }


    // import data to trips tabe
    public function import()
    {
        $sql =
            <<<SQL
            INSERT INTO trips
            SELECT * FROM s3(
                'https://datasets-documentation.s3.eu-west-3.amazonaws.com/nyc-taxi/trips_{1..2}.gz',
                'TabSeparatedWithNames', "
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
                `pickup_ntaname` String,
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
            ") SETTINGS input_format_try_infer_datetimes = 0;
        SQL;

        try {
            $this->clickhouse->write($sql);
            return 'import successfully completed';
        } catch (\Exception $e) {
            return "Error while importing data:" . $e->getMessage();
        }
    }



    // update small data
    public function update()
    {
        $sql = <<<SQL
            ALTER TABLE trips
            UPDATE pickup_ntaname ='Samarkand'
            WHERE passenger_count = 6
        SQL;
        try {
            $this->clickhouse->write($sql);
            $sqlCheck = "SELECT * FROM trips WHERE passenger_count = 6 LIMIT 10";
            try {
                $result = $this->clickhouse->select($sqlCheck);
                $count = $this->clickhouse->select('SELECT count(*) FROM trips WHERE passenger_count = 6;');
                return response()->json([$result->rows(), $count->rows()]);
            } catch (\Exception $e) {
                return response("Error: " . $e->getMessage(), 500);
            }
        } catch (\Exception $e) {
            return "Error while updating:" . $e->getMessage();
        }
    }



    // get updated infos for above method
    public function getUpdateInfo()
    {
        $sql = <<<SQL
        SELECT count(*) FROM trips WHERE pickup_ntaname = 'Samarkand';
        SQL;
        $result = $this->clickhouse->select($sql);
        return json_encode($result->rows());
    }



    // this method depends on mergetree engine and if runs with the
    // MergeTree will cause duplicating the data
    // so that make it  for
    // ==============ReplacingMergeTree===================
    // Engines

    public function update2()
    {
        $sql = <<<SQL
                    INSERT INTO trips
                    (
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
                        pickup_ntaname,
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
                    )
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
                        'Tashkent' AS pickup_ntaname,
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
                    FROM trips
                    WHERE passenger_count = 6;

                SQL;
        try {
            $this->clickhouse->write($sql);
            $check = $this->clickhouse->select("SELECT * FROM trips WHERE passenger_count = 6 AND pickup_ntaname = 'Tashkent' LIMIT 10;");
            $count = $this->clickhouse->select("SELECT count(*) FROM trips WHERE passenger_count = 6");
            return response()->json(['updated',  $count->rows()]);
        } catch (\Exception $e) {
            return "error:" . $e;
        }
    }
}
