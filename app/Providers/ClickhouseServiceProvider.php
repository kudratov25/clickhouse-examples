<?php

namespace App\Providers;

use ClickHouseDB\Client;
use Illuminate\Support\ServiceProvider;

class ClickhouseServiceProvider extends ServiceProvider
{
    /**
     * Register services.
     */
    public function register(): void
    {
        $this->app->singleton(Client::class, function ($pp) {
            $config = [
                'host' => env('CLICKHOUSE_HOST', '127.0.0.1'),
                'port' => env('CLICKHOUSE_PORT', '8123'),
                'username' => env('CLICKHOUSE_USERNAME', 'default'),
                'password' => env('CLICKHOUSE_PASSWORD', ''),
                'https' => env('CLICKHOUSE_HTTPS', false),
            ];
            $client = new Client($config);
            $client->database(env('CLICKHOUSE_DATABASE', 'default'));
            $client->setTimeout(10);
            $client->setConnectTimeOut(5);
            try {
                $client->ping(true);
            } catch (\Exception $e) {
                throw new \Exception("Error connecting to ClickHouse: " . $e->getMessage());
            }

            return $client;
        });
    }

    /**
     * Bootstrap services.
     */
    public function boot(): void
    {
        //
    }
}
