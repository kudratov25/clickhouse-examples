<?php

use App\Http\Controllers\ClickhouseControllers\ExController;
use App\Http\Controllers\ClickhouseControllers\UserController;
use App\Http\Controllers\Examples\TripsController;
use Illuminate\Support\Facades\Route;

Route::get('/', function () {
    return view('welcome');
});

Route::group(['prefix' => 'clickhouse'], function () {
    Route::get('/create-user', [UserController::class, 'createUsersTable']);

    // here routes for info{$table} and destroy{$table}
    Route::get('/info/{table}', [ExController::class, 'info']);
    Route::get('/destroy/{table}', [ExController::class, 'destroyTable']);

    // route for exmples
    Route::get('/trips/count', [TripsController::class, 'index']);
    Route::get('/trips/create', [TripsController::class, 'createTrips']);
    Route::get('/trips/import', [TripsController::class, 'import']);
    Route::get('/trips/update', [TripsController::class, 'update']);
    Route::get('/trips/get-update', [TripsController::class, 'getUpdateInfo']);
    Route::get('/trips/update2', [TripsController::class, 'update2']);

});
