<?php

use App\Helpers\DatabaseConnection;
use App\Http\Controllers\TestController;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Route;

/*
|--------------------------------------------------------------------------
| API Routes
|--------------------------------------------------------------------------
|
| Here is where you can register API routes for your application. These
| routes are loaded by the RouteServiceProvider within a group which
| is assigned the "api" middleware group. Enjoy building your API!
|
*/

Route::middleware('auth:sanctum')->get('/user', function (Request $request) {
    return $request->user();
});
//Route::get('/setEnv',[TestController::class,'setEnv']);
Route::get('/test2', [TestController::class, 'test2']);
// Route::get('/test_view',[TestController::class,'test_view']);
//Route::get('/editEnv/{id}',[TestController::class,'editEnv']);
Route::get('/test_connection', [TestController::class, 'test_connection']);

Route::get('/feed_test', function () {
    ini_set('max_execution_time', 1000);
    $source_connection = DB::connection('mysql2')->table('CT_CONNECTIONS')->find(81);
    $driver = $source_connection->connection_driver;
    $connection_name = $source_connection->connection_name;
    $src_connection = DatabaseConnection::setConnection($driver, $connection_name, 81);
    $test_query = $src_connection->select(DB::raw("SELECT * FROM CANDIDATE_SESSIONS"));
    $columns_array = [];
    $values_array = [];
    $columns_and_values =[];

    foreach ($test_query as $row) {
        $columns_array = [];
        $values_array = [];
        foreach ($row as $key => $val) {

            array_push($columns_array, $key);
            array_push($values_array, "'$val'");
        }
        $columns = implode(', ', $columns_array);
        $values = implode(', ', $values_array);
      //  array_push($columns_and_values,["'$key'" => "'$val'"]);
     //  $columns_and_values =  array_combine($columns_array,$values_array);
        $insert_query = DB::connection('mysql2')->statement(DB::raw("INSERT INTO DSS.CANDIDATE_SESSIONS_STG ($columns) VALUES ($values)"));

    }
    // return response()->json(["status" => "200", "message" => $values_array], 200);

    //dss.candidate_sessions_stg
    //session_marks_available_at

    return response()->json(["status" => "200", "message" => $insert_query], 200);
});
Route::get('/feed_job', function () {

    $feed_groups = [];
    $feed_ids = DB::connection('mysql2')->table('CT_FEEDS')->where('enabled', 1)->where('deleted_at', NULL)->pluck('id');
    $feed_id_grp = $feed_ids[0];
    //  $src_id = DB::connection('mysql2')->select("SELECT connection_id FROM CT_REP_GROUPS WHERE feed_id = $feed_id_grp AND deleted_at = NULL AND enabled = 1");

    foreach ($feed_ids as $feed_id) {
        // $fd_name = DB::connection('mysql2')->table('CT_FEEDS')->where('id', $feed_id)->pluck('feed_name');
        // $feed_name = str_replace(str_split('\\/:*?"<>|[]'), '', $fd_name);
        $group = DB::connection('mysql2')->select("SELECT * FROM CT_REP_GROUPS WHERE feed_id = $feed_id AND group_priority_feed > 0 AND group_priority_feed < 9000 AND deleted_at = NULL AND enabled = 1 ORDER BY id ASC");
        $feed_groups[$feed_id] = $group;
    }
    foreach ($feed_groups as $feed_id => $group) {
        $src_id = $group[$feed_id]->connection_id;
        $source_connection = DB::connection('mysql2')->table('CT_CONNECTIONS')->find($src_id);
        $driver = $source_connection->connection_driver;
        $connection_name = $source_connection->connection_name;
        $src_connection = DatabaseConnection::setConnection($driver, $connection_name, $src_id);

        $group_id = $group[$feed_id]->id;
        $procedureName = 'CT_GROUP_CALL';
        $status = null;
        $details = null;
        $exec_guid = null;
        $bindings = [
            'V_GROUP_ID_IN'  => $group_id,
            'V_STATUS_OUT' => [
                'value' => &$status,
                'length' => 1000,
            ],
            'V_DETAILS_OUT' => [
                'value' => &$details,
                'length' => 1000,
            ],
            'V_EXEC_OUT' => [
                'value' => &$exec_guid,
                'length' => 1000,
            ],
        ];

        $result = DB::connection('mysql2')->executeProcedure($procedureName, $bindings);
        if ($status == 71 || $status == 72) {
            continue;
        } elseif ($status != 0 && $status != 71  && $status != 72) {
            $error_details = DB::connection('mysql2')->table('CT_PROCESS_ERROR_LOG')->where('process_id', $exec_guid)->pluck('details')->first();
            //continue;
            return response()->json(["status" => "200", "message" => $error_details], 200);
        } else {
            $query = $details;
            // $test_query = $src_connection->statement(DB::raw("$query"));
            $test_query = $src_connection->statement(DB::raw("SELECT * FROM CANDIDATE_SESSIONS"));
            echo $test_query . "\n";
            return response()->json(["status" => "200", "message" => $status], 200);
        }
    }
    // return response()->json(["status" => "200", "message" => $feed_groups], 200);
});
