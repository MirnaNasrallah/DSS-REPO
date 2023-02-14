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
    $source_connection = DB::connection('oracle')->table('CT_CONNECTIONS')->find(81);
    $driver = $source_connection->connection_driver;
    $connection_name = $source_connection->connection_name;
    $src_connection = DatabaseConnection::setConnection($driver, $connection_name, 81);
    $test_query = $src_connection->select(DB::raw("SELECT * FROM CANDIDATE_SESSIONS WHERE ROWNUM < 5"));
    $columns_array = [];
    $values_array = [];
    $columns_and_values = [];

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
        $insert_query = DB::connection('oracle')->statement(DB::raw("INSERT INTO DSS.CANDIDATE_SESSIONS_STG ($columns) VALUES ($values)"));
    }
    $table_name = 'DSS.CANDIDATE_SESSIONS_STG';
    // return redirect()->route('view_group_output',[$table_name]);
    // return response()->json(["status" => "200", "message" => $values_array], 200);

    //dss.candidate_sessions_stg
    //session_marks_available_at

    return response()->json(["status" => "200", "message" => $test_query], 200);
});
Route::get('/feed_job', function () {

    $feed_groups = [];
    $feed_ids = DB::connection('oracle')->table('CT_FEEDS')->where('enabled', 1)->where('deleted_at', NULL)->orderBy('feed_sequence', 'asc')->pluck('id');
    $feed_id_grp = $feed_ids[0];
    //  $src_id = DB::connection('oracle')->select("SELECT connection_id FROM CT_REP_GROUPS WHERE feed_id = $feed_id_grp AND deleted_at = NULL AND enabled = 1");

    foreach ($feed_ids as $feed_id) {

        $group = DB::connection('oracle')->select("SELECT * FROM CT_REP_GROUPS WHERE feed_id = $feed_id AND group_priority_feed > 0 AND group_priority_feed < 9000 AND deleted_at IS NULL AND enabled = 1 ORDER BY group_priority_feed ASC");

        $feed_groups[$feed_id] = $group;
    }
    // return response()->json(["status" => "200", "message" => $feed_groups], 200);
    foreach ($feed_groups as $feed_id => $feed_group) {
        $time_now = new DateTime();
        $time = $time_now->format('Y-m-d H:i:s');
        echo "Feed $feed_id started at $time \n";
        //  return response()->json(["status" => "200", "message" => $group], 200);
        foreach ($feed_group as $group) {
            $group_name = $group->group_name;
            $group_owner = $group->group_owner;
            $table_name = substr($group_name, 0, -3);
            $owner_table_name = "$group_owner.$table_name";
            $staging_table = $owner_table_name . '_STG';

            $src_id = $group->connection_id;
            if($src_id == 999)
            {
                continue;
            }
            else{
                $source_connection = DB::connection('oracle')->table('CT_CONNECTIONS')->find($src_id);
                $driver = $source_connection->connection_driver;
                $connection_name = $source_connection->connection_name;
                $src_connection = DatabaseConnection::setConnection($driver, $connection_name, $src_id);

                $group_id = $group->id;
                echo "Executing the group $group_name" . "\n";
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

                $result = DB::connection('oracle')->executeProcedure($procedureName, $bindings);
                if ($status == 71 || $status == 72) {
                    continue;
                } elseif ($status != 0 && $status != 71  && $status != 72) {
                    $error_details = DB::connection('oracle')->table('CT_PROCESS_ERROR_LOG')->where('process_id', $exec_guid)->pluck('details')->first();
                    echo "group $group_name failed \n";
                    //continue;
                    //return response()->json(["status" => "200", "message" => $error_details], 200);
                } else {

                    $query = $details;
                    if ($query) {
                        $test_query = $src_connection->select(DB::raw("$query"));
                        $truncate_stg = DB::connection('oracle')->statement(DB::raw("TRUNCATE TABLE $staging_table"));
                        foreach ($test_query as $row) {


                            $columns_array = [];
                            $values_array = [];
                            foreach ($row as $key => $val) {

                                array_push($columns_array, $key);
                                array_push($values_array, "'$val'");
                            }
                            $columns = implode(', ', $columns_array);
                            $values = implode(', ', $values_array);

                            $insert_query = DB::connection('oracle')->statement(DB::raw("INSERT INTO $staging_table ($columns) VALUES ($values)"));
                        }
                        $time_now = new DateTime();
                        $time = $time_now->format('Y-m-d H:i:s');
                        echo "Group $group_name finished at $time \n ";
                    }else{
                        echo "query was empty in group $group_name \n";
                        continue;
                    }


                }
            }

        }
        $time_now = new DateTime();
        $time = $time_now->format('Y-m-d H:i:s');
        echo "Executed the Feed $feed_id at $time" . "\n";
    }
    return response()->json(["status" => "200", "message" => "All done!"], 200);
});
Route::get('/remove_priority', function () {
    DB::connection('oracle')->statement(DB::raw("UPDATE CT_REP_GROUPS SET group_priority_feed = NULL WHERE feed_id IS NULL"));
});
