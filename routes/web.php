<?php

use App\Helpers\DatabaseConnection;
use App\Http\Controllers\TestController;
use App\Http\Controllers\ConncetionController;
use App\Http\Controllers\TableController;
use App\Http\Controllers\GroupController;
use App\Http\Controllers\FeedController;
use Illuminate\Http\Request;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redirect;
use Illuminate\Support\Facades\Route;
use Illuminate\Support\Facades\Validator;
use RealRashid\SweetAlert\Facades\Alert;

/*
|--------------------------------------------------------------------------
| Web Routes
|--------------------------------------------------------------------------
|
| Here is where you can register web routes for your application. These
| routes are loaded by the RouteServiceProvider within a group which
| contains the "web" middleware group. Now create something great!
|
*/
####################################################################################
//------------------------------------VIEWS---------------------------------------//
####################################################################################

Route::get('/', function () {
    $connections = DB::connection('mysql2')->table('CT_CONNECTIONS')->select('*')->where('deleted_at', NULL)->get();
    return view('connectionsList', compact('connections'));
});
Route::get('/create_connection', function () {
    return view('createConnection');
})->name('createConnection');

Route::get('/edit_connection/{id}', function ($id) {
    $connections = DB::connection('mysql2')->table('CT_CONNECTIONS')->find($id);
    return view('editConnection', compact('connections'));
})->name('editConnection');

Route::get('/view_connections', function () {
    $connections = DB::connection('mysql2')->table('CT_CONNECTIONS')->select('*')->where('deleted_at', NULL)->get();
    return view('connectionsList', compact('connections'));
})->name('view_connections');

Route::get('/view_repGroups', function () {
    $groups = DB::connection('mysql2')->table('CT_REP_GROUPS')->orderBy('id', 'asc')->select('*')->where('deleted_at', NULL)->get();
    return view('repGroups', compact('groups'));
})->name('view_repGroups');

Route::get('/view_createdTables', function () {
    $tables = DB::connection('mysql2')->select("SELECT table_name, source_connection_id, rep_group_id FROM CT_MAPPINGS WHERE deleted_at IS NULL GROUP BY table_name, source_connection_id, rep_group_id");
    // dd($tables);
    return view('createdTablesList', compact('tables'));
})->name('view_createdTables');

Route::get('/view_connection/{id}', function ($id) {
    $connection = DB::connection('mysql2')->table('CT_CONNECTIONS')->find($id);
    return view('chooseTable', compact('id', 'connection'));
})->name('view_connection');

Route::get('/view_group/{id}', function ($id) {
    $group = DB::connection('mysql2')->table('CT_REP_GROUPS')->find($id);
    return view('view_group', compact('id', 'group'));
})->name('view_group');

Route::get('/view_feeds', function () {
    $feeds = DB::connection('mysql2')->table('CT_FEEDS')->orderBy('id', 'asc')->select('*')->where('deleted_at', NULL)->get();
    return view('viewFeeds', compact('feeds'));
})->name('view_feeds');

Route::get('/createTable/{id}/{schemasString}/{tablesString}/{group_mode}/{count}', function ($id, $schemasString, $tablesString, $group_mode, $count) {
    $schemas = explode(',', $schemasString);
    $tables = explode(',', $tablesString);
    $schemas_and_tables = [];
    for ($i = 0; $i < count($schemas);$i++)
    {
        $schema = $schemas[$i];
        $table = $tables[$i];
        array_push($schemas_and_tables, "$schema.$table");
    }
   // dd($schemas_and_tables,$count);

    $connection = DB::connection('mysql2')->table('CT_CONNECTIONS')->find($id);
    $driver = $connection->connection_driver;
    $connection_name = $connection->connection_name;
    $src_connection = DatabaseConnection::setConnection($driver, $connection_name, $id);
    $src_schema = DatabaseConnection::getSchema($driver, $connection_name, $id);

    $table_objects = DB::connection('mysql2')->select("SELECT OWNER || '.' || OBJECT_NAME AS SCHEMA_OBJECT FROM ALL_OBJECTS WHERE OWNER='DSS' AND (OBJECT_TYPE LIKE '%TABLE%' OR OBJECT_TYPE IN ('VIEW', 'FUNCTION')) AND OBJECT_NAME NOT LIKE 'CT_%'");

    $table_objs = [];
    foreach ($table_objects as $item) {
        array_push($table_objs,$item->schema_object);
    }
    $schemas_and_tables = array_merge($schemas_and_tables,$table_objs);
   // dd($schemas_and_tables);
    return view('createTable', compact('id', 'schemasString', 'tablesString', 'schemas_and_tables', 'src_schema', 'src_connection', 'count', 'group_mode'));
})->name('createTable');

Route::get('/view_rep_group/{src_id}/{new_table_name}', function ($src_id, $new_table_name) {

    return view('repModeSet', compact('src_id', 'new_table_name'));
})->name('view_rep_group');

Route::get('/editRepGroup/{id}', function ($id) {
    $group = DB::connection('mysql2')->table('CT_REP_GROUPS')->find($id);
    $new_table_name = DB::connection('mysql2')->table('CT_MAPPINGS')->where('rep_group_id', $id)->pluck('table_name')->first();
    return view('editRepGroup', compact('group', 'new_table_name'));
})->name('editRepGroup');

Route::get('/editTable/{table_name}/{col_count}/{group_mode}', function ($table_name, $col_count,$group_mode) {
    $rows = DB::connection('mysql2')->select("SELECT * FROM CT_MAPPINGS WHERE table_name = '$table_name'");
    $tables = DB::connection('mysql2')->select("SELECT table_name, source_connection_id, rep_group_id FROM CT_MAPPINGS WHERE table_name = '$table_name' GROUP BY table_name, source_connection_id, rep_group_id FETCH FIRST 1 ROWS ONLY");
    $table = $tables[0];
    $col_count = count($rows);
    $table_objects = DB::connection('mysql2')->select("SELECT OWNER || '.' || OBJECT_NAME AS SCHEMA_OBJECT FROM ALL_OBJECTS WHERE OWNER='DSS' AND (OBJECT_TYPE LIKE '%TABLE%' OR OBJECT_TYPE IN ('VIEW', 'FUNCTION')) AND OBJECT_NAME NOT LIKE 'CT_%'");

    $table_objs = [];
    foreach ($table_objects as $item) {
        array_push($table_objs, $item->schema_object);
    }
    return view('editTable', compact('table', 'rows', 'col_count','group_mode','table_objs'));
})->name('editTable');

Route::get('/editFeed/{id}', function ($id) {
    $feed = DB::connection('mysql2')->table('CT_FEEDS')->find($id);
    $groups = DB::connection('mysql2')->table('CT_REP_GROUPS')->where('feed_id', null)->where('deleted_at', null)->get();
    $used_groups = DB::connection('mysql2')->table('CT_REP_GROUPS')->where('feed_id', $id)->get();
    $count_groups = count($groups);

    return view('editFeed', compact('feed', 'groups', 'used_groups', 'count_groups'));
})->name('editFeed');
####################################################################################
//------------------------------------FUNCTIONS---------------------------------------//
####################################################################################
//DELETE
Route::get('/deleteConnection/{id}', function ($id) {
    $date = new DateTime();
    $result = $date->format('Y-m-d H:i:s');
    $result = str_replace(['-',' ',':'], "", $result);

    $con_name = DB::connection('mysql2')->table('CT_CONNECTIONS')->where('id', $id)->pluck('connection_name')->first();
    $connection_name = $result . '_' . $con_name;
    $tb_name = DB::connection('mysql2')->table('CT_MAPPINGS')->where('source_connection_id', $id)->pluck('table_name')->first();
    $grp_name = DB::connection('mysql2')->table('CT_REP_GROUPS')->where('connection_id', $id)->pluck('group_name')->first();
    $new_table_name = $result . '_' . $tb_name;
    $group_name = $result . '_' . $grp_name;
   // dd($tb_name, $grp_name);
    // $fd_name = DB::connection('mysql2')->table('CT_FEEDS')->where('connection_id', $id)->pluck('feed_name')->first();
    // $feed_name = $result . '_' . $fd_name;

    //  DB::connection('mysql2')->statement(DB::raw('UPDATE CT_CONNECTIONS SET deleted_at = NOW(), connection_name = "' . $result . '_' . $con_name . '"  WHERE id = "' . $id . '"'));
    $con_delete_stmt = "deleted_at = SYSDATE, connection_name = '" . $connection_name . "'";
    $table_delete_stmt = "rep_group_id = NULL, deleted_at = SYSDATE, table_name = '" . $new_table_name . "'  WHERE table_name =  '".$tb_name."' ";
    $group_delete_stmt = "feed_id = NULL, group_priority_feed = NULL, deleted_at = SYSDATE, group_name = '" . $group_name . "' WHERE group_name = '" . $grp_name . "'";
    DB::connection('mysql2')->statement(DB::raw('UPDATE CT_CONNECTIONS SET ' . $con_delete_stmt . ' WHERE id = ' . $id . ' '));
    DB::connection('mysql2')->statement(DB::raw("UPDATE CT_MAPPINGS SET $table_delete_stmt"));
    DB::connection('mysql2')->statement(DB::raw('UPDATE CT_REP_GROUPS SET ' . $group_delete_stmt . ''));
   //
    Alert::success('Success', 'Removed successfully');
    return redirect()->route('view_connections');
})->name('deleteConnection');

Route::get('/deletegroup/{id}', function ($id) {
    $date = new DateTime();
    $result = $date->format('Y-m-d H:i:s');
    $result = str_replace(['-', ' ', ':'], "", $result);

    $grp_name = DB::connection('mysql2')->table('CT_REP_GROUPS')->where('id', $id)->pluck('group_name')->first();
    $group_name = $result . '_' . $grp_name;
    //  DB::connection('mysql2')->statement(DB::raw('UPDATE CT_CONNECTIONS SET deleted_at = NOW(), connection_name = "' . $result . '_' . $con_name . '"  WHERE id = "' . $id . '"'));
    $delete_stmt = "deleted_at = SYSDATE, group_name = '" . $group_name . "'";
    DB::connection('mysql2')->statement(DB::raw('UPDATE CT_REP_GROUPS SET ' . $delete_stmt . ' WHERE id = ' . $id . ' '));
    //  DB::connection('mysql2')->update("UPDATE CT_CONNECTIONS SET deleted_at = SYSDATE, connection_name = ?, WHERE id = ?",["'$connection_name'","'$id'"]);
    Alert::success('Success', 'Removed successfully');
    return redirect()->route('view_repGroups');
})->name('deletegroup');

Route::get('/removeGroupFromFeed/{id}', function ($id) {
    $group = DB::connection('mysql2')->table('CT_REP_GROUPS')->find($id);
    DB::connection('mysql2')->statement(DB::raw("UPDATE CT_REP_GROUPS SET feed_id = NULL, group_priority_feed = NULL WHERE id = $id "));
    Alert::success('Success', 'Group Removed successfully');
    return redirect()->back();
});

Route::get('/deleteFeed/{id}', function ($id) {
    $date = new DateTime();
    $result = $date->format('Y-m-d H:i:s');
    $result = str_replace(['-', ' ', ':'], "", $result);

    $fd_name = DB::connection('mysql2')->table('CT_FEEDS')->where('id', $id)->pluck('feed_name')->first();
    $feed_name = $result . '_' . $fd_name;
    //  DB::connection('mysql2')->statement(DB::raw('UPDATE CT_CONNECTIONS SET deleted_at = NOW(), connection_name = "' . $result . '_' . $con_name . '"  WHERE id = "' . $id . '"'));
    $delete_stmt = "deleted_at = SYSDATE, feed_name = '" . $feed_name . "'";
    DB::connection('mysql2')->statement(DB::raw('UPDATE CT_FEEDS SET ' . $delete_stmt . ' WHERE id = ' . $id . ' '));
    DB::connection('mysql2')->statement(DB::raw("UPDATE CT_REP_GROUPS SET feed_id = NULL, group_priority_feed = NULL WHERE feed_id = " . $id . " "));
    //  DB::connection('mysql2')->update("UPDATE CT_CONNECTIONS SET deleted_at = SYSDATE, connection_name = ?, WHERE id = ?",["'$connection_name'","'$id'"]);
    Alert::success('Success', 'Removed successfully');
    return redirect()->route('view_feeds');
})->name('deleteFeed');

Route::get('/deleteTable/{table_name}', function ($table_name) {
    $date = new DateTime();
    $result = $date->format('Y-m-d H:i:s');
    $result = str_replace(['-', ' ', ':'], "", $result);

    $rep_group_id = DB::connection('mysql2')->table('CT_MAPPINGS')->where('table_name', $table_name)->pluck('rep_group_id')->first();

    $tb_name = DB::connection('mysql2')->table('CT_MAPPINGS')->where('table_name', $table_name)->pluck('table_name')->first();
    $grp_name = DB::connection('mysql2')->table('CT_REP_GROUPS')->where('id', $rep_group_id)->pluck('group_name')->first();
    $new_table_name = $result . '_' . $tb_name;

    $group_name = $result . '_' . $grp_name;
    //delete group
    $group_delete_stmt = "deleted_at = SYSDATE, feed_id = NULL, group_priority_feed = NULL";
    DB::connection('mysql2')->statement(DB::raw('UPDATE CT_REP_GROUPS SET ' . $group_delete_stmt . ' WHERE id = ' . $rep_group_id . ' '));
    //delete_table
    $delete_stmt = "deleted_at = SYSDATE, table_name = '" . $new_table_name . "', rep_group_id = NULL";
    DB::connection('mysql2')->statement(DB::raw("UPDATE CT_MAPPINGS SET $delete_stmt WHERE table_name = '" . $table_name . "' "));

    //  DB::connection('mysql2')->update("UPDATE CT_CONNECTIONS SET deleted_at = SYSDATE, connection_name = ?, WHERE id = ?",["'$connection_name'","'$id'"]);
    Alert::success('Success', 'Removed successfully');
    return redirect()->back();
})->name('deleteTable');

//CHECK USER'S PRIVILEGE

Route::get('/check_table_connection/{id}', function (Request $request, $id) {
    $count = 0;
    $data = $request->all();
    $validator = Validator::make(
        $data,
        [
            "GROUP_MODE" => "required",
            "schema_1" => "required|string",
            "schema_2" => "nullable|string",
            "schema_3" => "nullable|string",
            "schema_4" => "nullable|string",
            "schema_5" => "nullable|string",
            "table_1" => "required|string",
            "table_2" => "nullable|string",
            "table_3" => "nullable|string",
            "table_4" => "nullable|string",
            "table_5" => "nullable|string",


        ]
    );
    $schemas = [];
    $schemasString = "";
    $tables = [];
    $tablesString = "";
    if ($validator->fails()) {
        return Redirect::back()->withErrors($validator);
    } else {
        for ($i = 1; $i <= 5; $i++) {
            if ($data['schema_' . $i . ''] == null && $data['table_' . $i . ''] == null) {
                break;
            } else {
                // dd($data['schema_' . $i . '']);
                array_push($schemas, $data['schema_' . $i . '']);
                array_push($tables, $data['table_' . $i . '']);
            }
        }

        $trimmed_tables = array_map('trim', $tables);
        $trimmed_schemas = array_map('trim', $schemas);
        if (count($trimmed_tables) !== count(array_unique($trimmed_tables))) {
            //tables has duplicates
            $schemas_and_tables = array_combine($schemas, $tables);
        } elseif (count($trimmed_tables) == count(array_unique($trimmed_tables)) && count($trimmed_schemas) == count(array_unique($trimmed_schemas))) {
            $schemas_and_tables = array_combine($schemas, $tables);
        } else {
            $schemas_and_tables = array_combine($tables, $schemas);
        }
        $schemasString = implode(',', $trimmed_schemas);
        $tablesString = implode(',', $trimmed_tables);
        //   dd($trimmed_tables,$trimmed_schemas,$schemasString,$tablesString);

        // $schemas_and_tables = array_combine($trimmed_tables, $trimmed_schemas);
        // dd($schemas_and_tables,$trimmed_tables,$trimmed_schemas);
        if (count($trimmed_schemas) !== count(array_unique($trimmed_schemas))) {
            foreach ($schemas_and_tables as $tableName => $schema) {

                $connection = DB::connection('mysql2')->table('CT_CONNECTIONS')->find($id);
                // $tableName = $data['table_name'];
                // $schema = $data['schema'] ?? $connection->schema_name;
                //  DB::connection('mysql2')->statement(DB::raw("UPDATE CT_CONNECTIONS SET schema_name = '".$schema."' WHERE id = '".$id."'"));

                $driver = $connection->connection_driver;
                $connection_name = $connection->connection_name;
                $src_schema = DatabaseConnection::getSchema($driver, $connection_name, $id);
                $src_connection = DatabaseConnection::setConnection($driver, $connection_name, $id);
                try {
                    if ($driver == "oracle") {
                        $test_connection = $src_connection->statement(DB::raw('SELECT * FROM ' . $schema . '.' . $tableName . ' WHERE 1 = 2'));
                        //   dd(env('DB_CONNECTION'));
                    } else {
                        $test_connection = $src_connection->statement(DB::raw('SELECT * FROM ' . $schema . '.' . $tableName . ' WHERE 1 = 2'));
                        //    dd($test);
                    }
                } catch (Exception $e) {
                    //  DB::connection('mysql2')->statement(DB::raw('UPDATE CT_CONNECTIONS SET CONFIGURED = 0 WHERE ID = ' . $con->ID . ''));
                    Alert::error('Error', $e->getMessage())->autoClose(5000000);
                    return redirect()->back();
                }
            }
        } else {

            foreach ($schemas_and_tables as $schema => $tableName) {
                //   dd('SELECT * FROM ' . $schema . '.' . $tableName . ' WHERE 1 = 2');
                $connection = DB::connection('mysql2')->table('CT_CONNECTIONS')->find($id);
                // $tableName = $data['table_name'];
                // $schema = $data['schema'] ?? $connection->schema_name;
                //  DB::connection('mysql2')->statement(DB::raw("UPDATE CT_CONNECTIONS SET schema_name = '".$schema."' WHERE id = '".$id."'"));

                $driver = $connection->connection_driver;
                $connection_name = $connection->connection_name;
                $src_schema = DatabaseConnection::getSchema($driver, $connection_name, $id);
                $src_connection = DatabaseConnection::setConnection($driver, $connection_name, $id);
                try {
                    if ($driver == "oracle") {
                        $test_connection = $src_connection->statement(DB::raw('SELECT * FROM ' . $schema . '.' . $tableName . ' WHERE 1 = 2'));
                        //   dd(env('DB_CONNECTION'));
                    } else {
                        $test_connection = $src_connection->statement(DB::raw('SELECT * FROM ' . $schema . '.' . $tableName . ' WHERE 1 = 2'));
                        //    dd($test);
                    }
                } catch (Exception $e) {
                    //  DB::connection('mysql2')->statement(DB::raw('UPDATE CT_CONNECTIONS SET CONFIGURED = 0 WHERE ID = ' . $con->ID . ''));
                    Alert::error('Error', $e->getMessage())->autoClose(5000000);
                    return redirect()->back();
                }
            }
        }

        // array_map('strtoupper', array_values($schemas_and_tables));
        //   dd($schemas_and_tables);

        $count_array = [];
        if (count($trimmed_schemas) !== count(array_unique($trimmed_schemas))) {
            foreach ($schemas_and_tables as $tableName => $schema) {
                //  dd(strtoupper($tableName), $schema);.
                if ($driver == "oracle") {
                    $table = strtoupper($tableName);
                    $owner = strtoupper($schema);
                    $count = $count + count($src_connection->select("
                select * from all_tab_columns
                where table_name = '$table'
                and owner = '$owner'
                 "));
                    $count_array[$tableName . '_' . $schema] = count($src_connection->select("
                 select * from all_tab_columns
                 where table_name = '$table'
                 and owner = '$owner'
                  "));
                } else {
                    $count = $count + count($src_connection->select("
                   SELECT *
                   FROM information_schema.columns
                   WHERE table_name = '$tableName'
                   and table_schema = '$schema'
                 "));
                    $count_array[$tableName . '_' . $schema] = count($src_connection->select("
                 SELECT *
                   FROM information_schema.columns
                   WHERE table_name = '$tableName'
                   and table_schema = '$schema'
                  "));
                }
            }
        } else {
            foreach ($schemas_and_tables as $schema => $tableName) {
                //  dd(strtoupper($tableName), $schema);
                if ($driver == "oracle") {
                    $table = strtoupper($tableName);
                    $owner = strtoupper($schema);
                    $count = $count + count($src_connection->select("
                select * from all_tab_columns
                where table_name = '$table'
                and owner = '$owner'
                 "));
                    $count_array[$tableName . '_' . $schema] = count($src_connection->select("
                 select * from all_tab_columns
                 where table_name = '$table'
                 and owner = '$owner'
                  "));
                } else {
                    $count = $count + count($src_connection->select("
                   SELECT *
                   FROM information_schema.columns
                   WHERE table_name = '$tableName'
                   and table_schema = '$schema'
                 "));
                    $count_array[$tableName . '_' . $schema] = count($src_connection->select("
                 SELECT *
                   FROM information_schema.columns
                   WHERE table_name = '$tableName'
                   and table_schema = '$schema'
                  "));
                }
            }
        }
        $group_mode = $data['GROUP_MODE'];
        //  dd($count_array);
        if ($count == 0) {
            $count = 5;
        }
        Alert::success('Success', 'Connected successfully');
        //  return redirect()->back();
        return view('tablesList', compact('id', 'schemasString', 'tablesString', 'schemas_and_tables', 'count', 'src_schema', 'src_connection', 'count_array', 'trimmed_schemas', 'group_mode'));
    }
})->name('check_table_connection');



####################################################################################
//------------------------------------FUNCTION CALLS---------------------------------------//
####################################################################################

Route::get('/addToFeed', [TestController::class, 'addToFeed'])->name('addToFeed');
Route::get('/create_feed/{groups_ids}', [TestController::class, 'create_feed'])->name('create_feed');
Route::get('/set_new_table/{id}/{count}/{group_mode}', [TestController::class, 'set_new_table'])->name('set_new_table');
Route::get('update_table/{table_name}/{col_count}', [TestController::class, 'update_table'])->name('update_table');
Route::get('/create_rep_group/{id}/{new_table_name}', [TestController::class, 'create_rep_group'])->name('create_rep_group');
Route::get('/setEnv', [TestController::class, 'setEnv'])->name('setEnv');
Route::get('/editEnv/{id}', [TestController::class, 'editEnv'])->name('editEnv');
Route::get('/edit_group/{id}', [TestController::class, 'edit_group'])->name('edit_group');
Route::get('/updateFeed/{id}/{count_groups}', [TestController::class, 'updateFeed'])->name('updateFeed');
Route::get('/test_connection/{id}', [TestController::class, 'test_connection'])->name('test_connection');
