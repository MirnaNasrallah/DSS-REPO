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
        $source_connection = DB::connection('oracle')->table('CT_CONNECTIONS')->find($src_id);
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
                "ENABLED" => "boolean|nullable",
                "QUERY_CONDITION" =>  "nullable|string",

            ]
        );
        $query_condition = str_replace("'", "''", $data['QUERY_CONDITION']);
        $table = DB::connection('oracle')->table('CT_MAPPINGS')->where('table_name', $new_table_name)->first();


        if ($validator->fails()) {
            Alert::warning('Warning', 'There has been errors in the input data')->autoClose(5000000);;
            return Redirect::back()->withErrors($validator);
        } else {
            if (strtolower($dest_driver) == 'mysql') {
                DB::connection('oracle')->statement(DB::raw("UPDATE CT_REP_GROUPS SET enabled = '" . $data['ENABLED'] . "', frequency_minutes= '" . $data['FREQUENCY_MINUTES'] . "',batch_limit= '" . $data['BATCH_LIMIT'] . "',query_condition= '" . $query_condition . "', updated_at = NOW()  WHERE id = '" . $table->rep_group_id . "' "));
            } else {
                // dd($data['REP_MODE']);
                $update_statement = "enabled = '" . $data['ENABLED'] . "', frequency_minutes= '" . $data['FREQUENCY_MINUTES'] . "',batch_limit= '" . $data['BATCH_LIMIT'] . "',query_condition= '" . $query_condition . "', updated_at = SYSDATE";
                DB::connection('oracle')->statement(DB::raw("UPDATE CT_REP_GROUPS SET $update_statement WHERE id = '" . $table->rep_group_id . "' "));
            }

            try {
                if ($data['QUERY_CONDITION']) {
                    if (strtolower($driver) == "oracle") {

                        $test_connection = $src_connection->statement(DB::raw('SELECT 1 FROM DUAL ' . $data['QUERY_CONDITION'] . ''));
                        //   dd(env('DB_CONNECTION'));
                    } else {
                        $test_connection = $src_connection->statement(DB::raw('SELECT 1+1 ' . $data['QUERY_CONDITION'] . ''));
                        //    dd($test);
                    }
                }
                Alert::success('All done!', 'REP_GROUP Created successfully');
                return redirect()->route('view_repGroups');
            } catch (Exception $e) {
                Alert::error('Error', $e->getMessage())->autoClose(5000000);
                return redirect()->route('editRepGroup', [$table->rep_group_id]);
            }
        }
    }
    public function edit_group(Request $request, $rep_id)
    {
        $record = DB::connection('oracle')->table('CT_REP_GROUPS')->find($rep_id);
        $data = $request->all();
        $update_statement = [];
        $columns = [];
        $values = [];
        $data = array_map('strtoupper', $data);

        foreach ($data as $key => $value) {
            if (strtolower($key) == 'rep_mode') {

                $key = 'group_mode';
                array_push($columns, $key);
                array_push($values, $value);
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

        DB::connection('oracle')->statement(DB::raw("UPDATE CT_REP_GROUPS SET $update_stmt, updated_at = SYSDATE  WHERE id = $rep_id "));
        Alert::success('Success', 'REP_GROUP Edited successfully');
        return redirect()->route('view_repGroups');
    }
    public function removeGroupFromFeed($id)
    {
        $group = DB::connection('oracle')->table('CT_REP_GROUPS')->find($id);
        DB::connection('oracle')->statement(DB::raw("UPDATE CT_REP_GROUPS SET feed_id = NULL, group_priority_feed = NULL WHERE id = $id "));
        Alert::success('Success', 'Group Removed successfully');
        return redirect()->back();
    }
    public function deleteGroup($id)
    {
        $date = new DateTime();
        $result = $date->format('Y-m-d H:i:s');
        $result = str_replace(['-', ' ', ':'], "", $result);

        $grp_name = DB::connection('oracle')->table('CT_REP_GROUPS')->where('id', $id)->pluck('group_name')->first();
        $tb_name = DB::connection('oracle')->table('CT_MAPPINGS')->where('rep_group_id', $id)->pluck('table_name')->first();
        $group_name = $result . '_' . $grp_name;
        $new_table_name = $result . '_' . $tb_name;
        //  DB::connection('oracle')->statement(DB::raw('UPDATE CT_CONNECTIONS SET deleted_at = NOW(), connection_name = "' . $result . '_' . $con_name . '"  WHERE id = "' . $id . '"'));
        $delete_stmt = "group_name = '" . $group_name . "', feed_id = NULL, group_priority_feed = NULL, deleted_at = SYSDATE";
        DB::connection('oracle')->statement(DB::raw('UPDATE CT_REP_GROUPS SET ' . $delete_stmt . ' WHERE id = ' . $id . ' '));
        $table_delete_stmt = "deleted_at = SYSDATE, table_name = '" . $new_table_name . "', rep_group_id = NULL";
        DB::connection('oracle')->statement(DB::raw("UPDATE CT_MAPPINGS SET $table_delete_stmt WHERE table_name = '" . $tb_name . "' "));
        //  DB::connection('oracle')->update("UPDATE CT_CONNECTIONS SET deleted_at = SYSDATE, connection_name = ?, WHERE id = ?",["'$connection_name'","'$id'"]);
        Alert::success('Success', 'Removed successfully');
        return redirect()->route('view_repGroups');
    }
    public function view_group_output($table_name)
    {
        //TABLE NAME IS THE VIEW NAME
        $columns_names = [];
        $data_rows = [];
        $data_values = [];
        //  $table_name = 'DSS.CANDIDATE_SESSIONS_STG';
        $owner = strtok($table_name, '.');
        $table = strtok('');
        $limit = 100;
        $table_data = DB::connection('oracle')->select("SELECT * FROM $table_name WHERE ROWNUM < $limit");
        $limit = count($table_data);

        if ($table_data) {
            $columns = DB::connection('oracle')->select("
            select column_name from all_tab_columns
            where table_name = '$table'
            and owner = '$owner'
             ");
            //  dd("table: $table,owner: $owner");
            foreach ($columns as $col) {
                $column = strtolower($col->column_name);
                array_push($columns_names, $column);
                // dd($table_data[$key],$table_data[$key]->$column);
            }
            for ($key = 0; $key < $limit; $key++) {
                foreach ($columns_names as $column) {
                    if (array_key_exists($key, $table_data)) {
                        $data_rows[$column] = $table_data[$key]->$column;
                    }
                }
                $data_values[$key] = $data_rows;
            }
            //  dd($data_values, $columns_names, $table_name);
            return view('view_group_output', compact('data_values', 'columns_names', 'table_name'));
        } else {
            Alert::warning('Hold On!', 'The table was not loaded yet');
            return redirect()->back();
        }


        //    dd($data_values);

    }
}
