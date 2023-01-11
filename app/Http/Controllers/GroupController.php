<?php

namespace App\Http\Controllers;

use App\Helpers\DatabaseConnection;
use RealRashid\SweetAlert\Facades\Alert;

use Carbon\Carbon;
use DateTime;
use Exception;
use Illuminate\Broadcasting\Broadcasters\RedisBroadcaster;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Config;
// use Illuminate\Database\Connection;

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Hash;
use Illuminate\Support\Facades\Redirect;
use Illuminate\Support\Facades\Schema;
use PDO;
use PhpParser\Node\Stmt\TryCatch;
use Illuminate\Support\Facades\Validator;
use Illuminate\Validation\Rule;


class GroupController extends Controller
{
    public function create_rep_group(Request $request, $src_id, $new_table_name)
    {
        $source_connection = DB::connection('mysql2')->table('CT_CONNECTIONS')->find($src_id);
        $dest_driver = env('DB_CONNECTION_SECOND');
        $driver = $source_connection->connection_driver;
        $connection_name = $source_connection->connection_name;
        $src_schema = DatabaseConnection::getSchema($driver, $connection_name, $src_id);
        $src_connection = DatabaseConnection::setConnection($driver, $connection_name, $src_id);

        $data = $request->all();
        $validator = Validator::make(
            $data,
            [
                "FREQUENCY_MINUTES" => "required|numeric",
                "BATCH_LIMIT" => "nullable|numeric",
                "REP_MODE" => "nullable|string",
                "ENABLED" => "boolean|nullable",
                "QUERY_CONDITION" =>  "nullable|string",

            ]
        );
        $data = array_map('strtoupper', $data);
        $table = DB::connection('mysql2')->table('CT_MAPPINGS')->where('table_name', $new_table_name)->first();
        // if ($table->lookup_table == 1) {
        //     $group_name = strtoupper($new_table_name . "_FF");
        // } else {
        //     $group_name = strtoupper($new_table_name . "_DR");
        // }
        if ($validator->fails()) {
            Alert::warning('Warning', 'There has been errors in the input data')->autoClose(5000000);;
            return Redirect::back()->withErrors($validator);
        } else {
            if ($dest_driver == 'mysql') {
                DB::connection('mysql2')->statement(DB::raw("UPDATE CT_REP_GROUPS SET enabled = '" . $data['ENABLED'] . "', group_mode = '" . $data['REP_MDOE'] . "',frequency_minutes= '" . $data['FREQUENCY_MINUTES'] . "',batch_limit= '" . $data['BATCH_LIMIT'] . "',query_condition= '" . $data['QUERY_CONDITION'] . "', updated_at = NOW()  WHERE id = '" . $table->rep_group_id . "' "));
                // $insert_stmt = "insert ignore into CT_REP_GROUPS (group_name, enabled, mode, connection_id, frequency_minutes, batch_limit, query_condition, created_at, updated_at) values ('" . $group_name . "','" . $data['ENABLED'] . "','" . $data['REP_MDOE'] . "','" . $src_id . "','" . $data['FREQUENCY_MINUTES'] . "','" . $data['BATCH_LIMIT'] . "','" . $data['QUERY_CONDITION'] . "', NOW(), NOW())";
                // DB::connection('mysql2')->insert($insert_stmt);
                // $new_rep_group_id = DB::connection('mysql2')->table('CT_REP_GROUPS')->latest()->pluck('id')->first();
                // DB::connection('mysql2')->statement(DB::raw('UPDATE CT_MAPPINGS SET rep_group_id = "' . $new_rep_group_id . '" WHERE table_name  = ' . $new_table_name . ' '));
            } else {
                // dd($data['REP_MODE']);
                $update_statement = "enabled = '" . $data['ENABLED'] . "', frequency_minutes= '" . $data['FREQUENCY_MINUTES'] . "',batch_limit= '" . $data['BATCH_LIMIT'] . "',query_condition= '" . $data['QUERY_CONDITION'] . "', updated_at = SYSDATE";
                DB::connection('mysql2')->statement(DB::raw("UPDATE CT_REP_GROUPS SET $update_statement WHERE id = '" . $table->rep_group_id . "' "));
                // $insert_stmt = "insert into CT_REP_GROUPS (group_name, enabled, rep_mode, connection_id, frequency_minutes, batch_limit, query_condition, created_at, updated_at) values ('" . $group_name . "','" . $data['ENABLED'] . "','" . $data['REP_MODE'] . "','" . $src_id . "','" . $data['FREQUENCY_MINUTES'] . "','" . $data['BATCH_LIMIT'] . "','" . $data['QUERY_CONDITION'] . "', SYSDATE, SYSDATE)";
                // // $insert_stmt = "insert into CT_REP_GROUPS values ('" . $group_name . "','" . $data['ENABLED'] . "','" . $data['REP_MODE'] . "','" . $src_id . "','" . $data['FREQUENCY_MINUTES'] . "','" . $data['BATCH_LIMIT'] . "','" . $data['QUERY_CONDITION'] . "', SYSDATE, SYSDATE)";
                // DB::connection('mysql2')->insert($insert_stmt);
                // $new_rep_group_id = DB::connection('mysql2')->table('CT_REP_GROUPS')->latest()->pluck('id')->first();
                // $update_statement = "UPDATE CT_MAPPINGS SET rep_group_id = '" . $new_rep_group_id . "' WHERE table_name  = '" . $new_table_name . "' ";
                // DB::connection('mysql2')->statement(DB::raw($update_statement));
            }

            try {
                if ($data['QUERY_CONDITION']) {
                    if ($driver == "oracle") {

                        $test_connection = $src_connection->statement(DB::raw('SELECT 1 FROM DUAL WHERE ' . $data['QUERY_CONDITION'] . ''));
                        //   dd(env('DB_CONNECTION'));
                    } else {
                        $test_connection = $src_connection->statement(DB::raw('SELECT 1+1 WHERE ' . $data['QUERY_CONDITION'] . ''));
                        //    dd($test);
                    }
                }
                Alert::success('All done!', 'REP_GROUP Created successfully');
                return redirect()->route('view_repGroups');
            } catch (Exception $e) {
                Alert::error('Error', $e->getMessage())->autoClose(5000000);
                return redirect()->back();
            }
        }
    }
    public function edit_group(Request $request, $rep_id)
    {
        $record = DB::connection('mysql2')->table('CT_REP_GROUPS')->find($rep_id);
        $data = $request->all();
        $update_statement = [];
        $columns = [];
        $values = [];
        $data = array_map('strtoupper', $data);

        foreach ($data as $key => $value) {
            if ($key == 'group_name') {
                $FF = strpos($data['group_name'], '_FF');
                $DR = strpos($data['group_name'], '_DR');
                if ($FF) {
                    $value = $value . "_FF";
                    array_push($columns, $key);
                    array_push($values, $value);
                } elseif ($DR) {
                    $value = $value . "_DR";
                    array_push($columns, $key);
                    array_push($values, $value);
                }
            }

            if ($value === null) {
                array_push($columns, $key);
                array_push($values, 'NULL');
                //    }

            } elseif ($value !== null && $key != "group_name") {
                array_push($columns, $key);
                array_push($values, $value);
            }
        }
        $col_and_data = array_combine($columns, $values);

        foreach ($col_and_data as $key => $value) {

            if ($value == "NULL") {
                array_push($update_statement, "" . $key . " = '" . $record->$key . "'");
            } else {

                $update = $value;
                array_push($update_statement, "" . $key . " = '" . $update . "'");
            }
        }
        $update_stmt = implode(', ', $update_statement);

        DB::connection('mysql2')->statement(DB::raw("UPDATE CT_REP_GROUPS SET $update_stmt, updated_at = SYSDATE  WHERE id = $rep_id "));
        Alert::success('Success', 'REP_GROUP Edited successfully');
        return redirect()->route('view_repGroups');
    }
    public function removeGroupFromFeed($id)
    {
        $group = DB::connection('mysql2')->table('CT_REP_GROUPS')->find($id);
        DB::connection('mysql2')->statement(DB::raw("UPDATE CT_REP_GROUPS SET feed_id = NULL, group_priority_feed = NULL WHERE id = $id "));
        Alert::success('Success', 'Group Removed successfully');
        return redirect()->back();
    }
    public function deleteGroup($id)
    {
        $date = new DateTime();
        $result = $date->format('Y-m-d H:i:s');
        $result = str_replace(['-', ' ', ':'], "", $result);

        $grp_name = DB::connection('mysql2')->table('CT_REP_GROUPS')->where('id', $id)->pluck('group_name')->first();
        $group_name = $result . '_' . $grp_name;
        //  DB::connection('mysql2')->statement(DB::raw('UPDATE CT_CONNECTIONS SET deleted_at = NOW(), connection_name = "' . $result . '_' . $con_name . '"  WHERE id = "' . $id . '"'));
        $delete_stmt = "group_name = '" . $group_name . "', feed_id = NULL, group_priority_feed = NULL, deleted_at = SYSDATE";
        DB::connection('mysql2')->statement(DB::raw('UPDATE CT_REP_GROUPS SET ' . $delete_stmt . ' WHERE id = ' . $id . ' '));
        //  DB::connection('mysql2')->update("UPDATE CT_CONNECTIONS SET deleted_at = SYSDATE, connection_name = ?, WHERE id = ?",["'$connection_name'","'$id'"]);
        Alert::success('Success', 'Removed successfully');
        return redirect()->route('view_repGroups');
    }
}
