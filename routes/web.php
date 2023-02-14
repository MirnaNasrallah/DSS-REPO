<?php

use App\Helpers\DatabaseConnection;
use App\Http\Controllers\TestController;
use App\Http\Controllers\ConncetionController;
use App\Http\Controllers\ConnectionController;
use App\Http\Controllers\TableController;
use App\Http\Controllers\GroupController;
use App\Http\Controllers\FeedController;
use Illuminate\Http\Request;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Redirect;
use Illuminate\Support\Facades\Route;
use Illuminate\Support\Facades\Schema;
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
    $connections = DB::connection('oracle')->table('CT_CONNECTIONS')->select('*')->where('deleted_at', NULL)->get();
    return view('Connections_pages/connectionsList', compact('connections'));
});


//---------------------------CONNECTIONS----------------------------//
Route::get('/view_connections', function () {
    $connections = DB::connection('oracle')->table('CT_CONNECTIONS')->select('*')->where('deleted_at', NULL)->get();
    return view('Connections_pages/connectionsList', compact('connections'));
})->name('view_connections');

Route::get('/view_connection/{id}', function ($id) {
    $connection = DB::connection('oracle')->table('CT_CONNECTIONS')->find($id);
    return view('Tables_pages/chooseTable', compact('id', 'connection'));
})->name('view_connection');

Route::get('/create_connection', function () {
    return view('Connections_pages/createConnection');
})->name('createConnection');

Route::get('/edit_connection/{id}', function ($id) {
    $connections = DB::connection('oracle')->table('CT_CONNECTIONS')->find($id);
    return view('Connections_pages/editConnection', compact('connections'));
})->name('editConnection');

//---------------------------TABLES----------------------------//

Route::get('/view_createdTables', function () {
    $tables = DB::connection('oracle')->select("SELECT table_name, source_connection_id, rep_group_id FROM CT_MAPPINGS WHERE column_id != 0 AND deleted_at IS NULL GROUP BY table_name, source_connection_id, rep_group_id");
    // foreach($tables as $table)
    // {
    //     $con_id = $table->source_connection_id;
    //     DB::connection('oracle')->statement(DB::raw("UPDATE CT_MAPPINGS SET source_connection_id = $con_id WHERE  NVL(source_connection_id, 0) = 0 AND column_id = 0 AND table_name = '".$table->table_name."'"));
    // }

    // $tables = DB::connection('oracle')->select("SELECT table_name, source_connection_id, rep_group_id FROM CT_MAPPINGS WHERE deleted_at IS NULL GROUP BY table_name, source_connection_id, rep_group_id");
    return view('Tables_pages/createdTablesList', compact('tables'));
})->name('view_createdTables');


Route::get('/createTable/{id}/{schemasString}/{tablesString}/{group_mode}/{count}', function ($id, $schemasString, $tablesString, $group_mode, $count) {
    $schemas = explode(',', $schemasString);
    $tables = explode(',', $tablesString);
    $schemas_and_tables = [];
    for ($i = 0; $i < count($schemas); $i++) {
        $schema = $schemas[$i];
        $table = $tables[$i];
        array_push($schemas_and_tables, "$schema.$table");
    }
    // dd($schemas_and_tables,$count);

    $connection = DB::connection('oracle')->table('CT_CONNECTIONS')->find($id);
    $driver = $connection->connection_driver;
    $connection_name = $connection->connection_name;
    $src_connection = DatabaseConnection::setConnection($driver, $connection_name, $id);
    $src_schema = DatabaseConnection::getSchema($driver, $connection_name, $id);

    $table_objects = DB::connection('oracle')->select("SELECT OWNER || '.' || OBJECT_NAME AS SCHEMA_OBJECT FROM ALL_OBJECTS WHERE OWNER='DSS' AND (OBJECT_TYPE LIKE '%TABLE%' OR OBJECT_TYPE IN ('VIEW', 'FUNCTION')) AND OBJECT_NAME NOT LIKE 'CT_%'");

    $table_objs = [];
    foreach ($table_objects as $item) {
        array_push($table_objs, $item->schema_object);
    }
    $schemas_and_tables = array_merge($schemas_and_tables, $table_objs);
    // dd($schemas_and_tables);
    return view('Tables_pages/createTable', compact('id', 'schemasString', 'tablesString', 'schemas_and_tables', 'src_schema', 'src_connection', 'count', 'group_mode'));
})->name('createTable');

Route::get('/editTable/{table_name}/{col_count}/{group_mode}', function ($table_name, $col_count, $group_mode) {
    $rows = DB::connection('oracle')->select("SELECT * FROM CT_MAPPINGS WHERE table_name = '$table_name'");
    $tables = DB::connection('oracle')->select("SELECT table_name, source_connection_id, rep_group_id FROM CT_MAPPINGS WHERE table_name = '$table_name' GROUP BY table_name, source_connection_id, rep_group_id FETCH FIRST 1 ROWS ONLY");
    $table = $tables[0];
    $col_count = count($rows);
    $table_objects = DB::connection('oracle')->select("SELECT OWNER || '.' || OBJECT_NAME AS SCHEMA_OBJECT FROM ALL_OBJECTS WHERE OWNER='DSS' AND (OBJECT_TYPE LIKE '%TABLE%' OR OBJECT_TYPE IN ('VIEW', 'FUNCTION')) AND OBJECT_NAME NOT LIKE 'CT_%'");

    $table_objs = [];
    foreach ($table_objects as $item) {
        array_push($table_objs, $item->schema_object);
    }
    return view('Tables_pages/editTable', compact('table', 'rows', 'col_count', 'group_mode', 'table_objs'));
})->name('editTable');

//---------------------------GROUPS----------------------------//
##--VIEW ALL--##
Route::get('/view_repGroups', function () {
    $groups = DB::connection('oracle')->table('CT_REP_GROUPS')->orderBy('id', 'asc')->select('*')->where('deleted_at', NULL)->get();
    return view('Group_pages/repGroups', compact('groups'));
})->name('view_repGroups');



##--CREATE A GROUP--##
Route::get('/view_rep_group/{src_id}/{new_table_name}', function ($src_id, $new_table_name) {
    return view('Group_pages/repModeSet', compact('src_id', 'new_table_name'));
})->name('view_rep_group');

##--EDIT GROUP--##
Route::get('/editRepGroup/{id}', function ($id) {
    $group = DB::connection('oracle')->table('CT_REP_GROUPS')->find($id);
    $new_table_name = DB::connection('oracle')->table('CT_MAPPINGS')->where('rep_group_id', $id)->pluck('table_name')->first();
    return view('Group_pages/editRepGroup', compact('group', 'new_table_name'));
})->name('editRepGroup');

//---------------------------FEEDS----------------------------//
Route::get('/view_feeds', function () {
    $feed_groups = [];
    $feeds = DB::connection('oracle')->table('CT_FEEDS')->orderBy('id', 'asc')->select('*')->where('deleted_at', NULL)->get();
    foreach ($feeds as $feed) {
        $feed_id = $feed->id;
      //  $used_groups = DB::connection('oracle')->table('CT_REP_GROUPS')->where('feed_id', $feed_id)->get();
        $used_groups = DB::connection('oracle')->select("SELECT group_name FROM CT_REP_GROUPS WHERE feed_id = $feed_id");
        if (count($used_groups) == 0) {
            DB::connection('oracle')->statement(DB::raw("UPDATE CT_FEEDS SET enabled = 0 WHERE id = $feed_id "));
        } else {
            DB::connection('oracle')->statement(DB::raw("UPDATE CT_FEEDS SET enabled = 1 WHERE id = $feed_id "));
        }
    }

    $feeds = DB::connection('oracle')->table('CT_FEEDS')->orderBy('id', 'asc')->select('*')->where('deleted_at', NULL)->get();
    return view('Feeds_pages/viewFeeds', compact('feeds'));
})->name('view_feeds');

Route::get('/editFeed/{id}', function ($id) {
    $feed = DB::connection('oracle')->table('CT_FEEDS')->find($id);
    $groups = DB::connection('oracle')->table('CT_REP_GROUPS')->where('feed_id', null)->where('deleted_at', null)->get();
    $used_groups = DB::connection('oracle')->table('CT_REP_GROUPS')->where('feed_id', $id)->get();
    $count_groups = count($groups);

    return view('Feeds_pages/editFeed', compact('feed', 'groups', 'used_groups', 'count_groups'));
})->name('editFeed');

####################################################################################
//--------------------------------FUNCTION CALLS-----------------------------------//
####################################################################################
Route::get('/dashboard', function () {
    $connections =count(DB::connection('oracle')->table('CT_CONNECTIONS')->select('*')->where('deleted_at', NULL)->get()) ?? 'N/A';
    $completed_feeds = count(DB::connection('oracle')->table('CT_FEED_EXE_FEEDBACK')->select('*')->where('status', 'Complete')->get()) ?? 'N/A';
    $skipped_feeds = count(DB::connection('oracle')->table('CT_FEED_EXE_FEEDBACK')->select('*')->where('status', 'Skipped')->get()) ?? 'N/A';
    $stopped_feeds = count(DB::connection('oracle')->table('CT_FEED_EXE_FEEDBACK')->select('*')->where('status', 'Stopped')->get()) ?? 'N/A';
    $failed_feeds = count(DB::connection('oracle')->table('CT_FEED_EXE_FEEDBACK')->select('*')->where('status', 'Failed')->get()) ?? 'N/A';
    return view('dashboard',compact('connections','completed_feeds','skipped_feeds','stopped_feeds','failed_feeds'));
});

////---------------------------------CONNECTIONS------------------------------//////////
Route::get('/setEnv', [ConnectionController::class, 'setEnv'])->name('setEnv');
Route::get('/editEnv/{id}', [ConnectionController::class, 'editEnv'])->name('editEnv');
Route::get('/deleteConnection/{id}', [ConnectionController::class, 'deleteConnection'])->name('deleteConnection');

////---------------------------------TABLES--------------------------------/////////
Route::get('/set_new_table/{id}/{count}/{group_mode}', [TableController::class, 'set_new_table'])->name('set_new_table');
Route::get('update_table/{table_name}/{col_count}', [TableController::class, 'update_table'])->name('update_table');
Route::get('/check_table_connection/{id}', [TableController::class, 'check_table_connection'])->name('check_table_connection');
Route::get('/deleteTable/{table_name}', [TableController::class, 'deleteTable'])->name('deleteTable');

/////--------------------------------GROUPS----------------------------------/////////

Route::get('/create_rep_group/{id}/{new_table_name}', [GroupController::class, 'create_rep_group'])->name('create_rep_group');
Route::get('/edit_group/{id}', [GroupController::class, 'edit_group'])->name('edit_group');
Route::get('/deletegroup/{id}', [GroupController::class, 'deleteGroup'])->name('deletegroup');
Route::get('/removeGroupFromFeed/{id}',  [GroupController::class, 'removeGroupFromFeed'])->name('removeGroupFromFeed');
Route::get('/view_group_output/{table_name}', [GroupController::class,'view_group_output'])->name('view_group_output');

/////////-----------------------------FEEDS----------------------------------------/////////
Route::get('/addToFeed', [FeedController::class, 'addToFeed'])->name('addToFeed');
Route::get('/create_feed/{groups_ids}', [FeedController::class, 'create_feed'])->name('create_feed');
Route::get('/updateFeed/{id}/{count_groups}', [FeedController::class, 'updateFeed'])->name('updateFeed');
Route::get('/deleteFeed/{id}', [FeedController::class, 'deleteFeed'])->name('deleteFeed');
Route::get('/feed_executer', [FeedController::class, 'feed_executer'])->name('feed_executer');
