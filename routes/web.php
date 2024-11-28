<?php

use App\Http\Controllers\ClickhouseControllers\ExController;
use App\Http\Controllers\ClickhouseControllers\UserController;
use App\Http\Controllers\Examples\JoinsController;
use App\Http\Controllers\Examples\TripsController;
use Illuminate\Support\Facades\Route;

Route::get('/', function () {
    return view('welcome');
});

Route::group(['prefix' => 'clickhouse'], function () {

    Route::get('/create-user', [UserController::class, 'createUsersTable']);
    Route::get('/create-user/indexed', [UserController::class, 'createIndexedUsersTable']);
    Route::get('/create-user/indexed/optimize', [UserController::class, 'optimizeIndexedUsersTable']);

    // here routes for info{$table} and destroy{$table}
    Route::get('/info/{table}', [ExController::class, 'info']);
    Route::get('/destroy/{table}', [ExController::class, 'destroyTable']);

    // route for exmples
    Route::group(['prefix' => 'trips'], function () {
        Route::get('/count', [TripsController::class, 'index']);
        Route::get('/create', [TripsController::class, 'createTrips']);
        Route::get('/import', [TripsController::class, 'import']);
        Route::get('/update', [TripsController::class, 'update']);
        Route::get('/get-update', [TripsController::class, 'getUpdateInfo']);
        Route::get('/update2', [TripsController::class, 'update2']);
        Route::get('/materialized-view', [TripsController::class, 'makeView']);
        Route::get('/materialized-view/data', [TripsController::class, 'showMvTable']);
    });

    Route::group(['prefix' => 'joins'], function () {
        Route::get('/new_trips/create', [JoinsController::class, 'createTripsTable']);
        Route::get('/new_cities/create', [JoinsController::class, 'createCitiesTable']);
        Route::get('/new_cities/seed', [JoinsController::class, 'seedCitiesTable']);
        Route::get('/new_trips/seed', [JoinsController::class, 'seedNewTripsTable']);
        Route::get('/new_trips/innerjoin', [JoinsController::class, 'innerjoin']);
    });
});
