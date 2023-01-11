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


class TableController extends Controller
{
    public function set_new_table(Request $request, $src_id, $count, $group_mode)
    {
        $table_owner = 'DSS';

        $source_connection = DB::connection('mysql2')->table('CT_CONNECTIONS')->find($src_id);

        $driver = $source_connection->connection_driver;
        $connection_name = $source_connection->connection_name;
        $src_schema = DatabaseConnection::getSchema($driver, $connection_name, $src_id);
        $src_connection = DatabaseConnection::setConnection($driver, $connection_name, $src_id);
        $new_table_name = $request->TABLE_NAME;
        $col_names = []; //-->UNIQUE
        $type_names = [];
        $id_names = []; //-->UNIQUE ORDER
        $rep_names = [];
        $idCol_names = []; //--->MUST NOT HAVE TWO VALUES OF 1
        $comp_names = [];
        $enabled_names = [];
        $src_column_names = [];
        $src_column_types = [];
        $schema_table_names = [];
        //  dd($count);
        foreach (range(0, $count - 1) as $i) {
            $col_name = "COLUMN_NAME_$i";
            if ($request->$col_name != null && $request->$col_name != "null") {
                $new_count = $i;
                array_push($col_names, $request->$col_name);
            }
            $type_name = "COLUMN_TYPE_$i";
            if ($request->$type_name != null && $request->$type_name != "null") {
                $new_count = $i;
                array_push($type_names, $request->$type_name);
            }
            $id_name = "COLUMN_ID_$i";
            if ($request->$id_name != null && $request->$id_name != "null") {
                $new_count = $i;
                array_push($id_names, $request->$id_name);
            }
            $repMode_name = "REP_MODE_$i";
            if ($request->$repMode_name != null && $request->$repMode_name != "null") {
                $new_count = $i;
                array_push($rep_names, $request->$repMode_name);
            }
            $idCol_name = "IDENTITY_COLUMN_$i";
            if ($request->$idCol_name != null && $request->$idCol_name != "null") {

                array_push($idCol_names, $request->$idCol_name);
            }
            $comp_name = "DATE_RANGE_COMPARISON_$i";
            if ($request->$comp_name != null && $request->$comp_name != "null") {

                array_push($comp_names, $request->$comp_name);
            }
            $enabled_name = "ENABLED_$i";
            if ($request->$enabled_name != null && $request->$enabled_name != "null") {

                array_push($enabled_names, $request->$enabled_name);
            }
            $src_column_name = "SOURCE_COLUMN_NAME_$i";
            if ($request->$src_column_name != null && $request->$src_column_name != "null") {
                $new_count = $i;
                array_push($src_column_names, $request->$src_column_name);
            }
            $src_column_type = "SOURCE_COLUMN_TYPE_$i";
            if ($request->$src_column_type != null && $request->$src_column_type != "null") {
                $new_count = $i;
                array_push($src_column_types, $request->$src_column_type);
            }
            $schema_table_name = "SCHEMA_TABLE_$i";

            if ($request->$schema_table_name !== null && $request->$schema_table_name !== "null") {
                $new_count = $i;
                array_push($schema_table_names, $request->$schema_table_name);
            }
        }

        if (count($col_names) !== count(array_unique($col_names))) {
            //array has duplicates
            Alert::warning('Warning', 'Duplicate column names')->autoClose(5000000);
            return redirect()->back();
        } elseif (count($id_names) !== count(array_unique($id_names)) && min($id_names) >= 0) {
            Alert::warning('Warning', 'Duplicate Or Negative column order')->autoClose(5000000);
            return redirect()->back();
        } else {
            $table_array = [];
            $group_name = "";

            // dd($count, $new_count);
            if ($col_names && $type_names && $id_names && $rep_names && $idCol_names && $comp_names && $enabled_names && $src_column_names && $src_column_types && $schema_table_names) {
                $new_count = $new_count + 1;
                for ($i = 0; $i < $new_count; $i++) {
                    for ($j = 0; $j < $new_count; $j++) {
                        if (array_key_exists($i, $col_names) && array_key_exists($i, $type_names) && array_key_exists($i, $id_names) && array_key_exists($i, $rep_names) && array_key_exists($i, $idCol_names) && array_key_exists($i, $comp_names) && array_key_exists($i, $enabled_names) && array_key_exists($i, $src_column_names) && array_key_exists($i, $src_column_types) && array_key_exists($i, $schema_table_names)) {
                            $schema_table = preg_split("/\./", $schema_table_names[$i]);
                            //    dd($schema_table[0], $schema_table[1]);                                                                                                                                        //src_schema,src_table,src_column,src_column_type,rep_mode,rep_group_id
                            // $table_array[$i] = [$new_table_name, $col_names[$i], $id_names[$i], $type_names[$i], $enabled_names[$i], $idCol_names[$i], $request->lookup_table, $comp_names[$i], $src_id, $schema_table[0], $schema_table[1], $src_column_names[$i], $src_column_types[$i], $rep_names[$i]];
                            $table_array[$i] = [$new_table_name, $col_names[$i], $id_names[$i], $type_names[$i], $enabled_names[$i], $idCol_names[$i], $comp_names[$i], $src_id, $schema_table[0], $schema_table[1], $src_column_names[$i], $src_column_types[$i], $rep_names[$i]];
                        }
                    }
                }
            } else {
                Alert::warning('Warning', 'Too Few Columns');
                return redirect()->back();
            }
            $new_table_name = strtoupper($new_table_name);
            if ($group_mode == 'FULL_FEED') {
                $group_name = strtoupper($new_table_name . "_FF");
            } elseif ($group_mode == 'DATE_RANGE') {
                $group_name = strtoupper($new_table_name . "_DR");
            } elseif ($group_mode == 'AUTO') {
                $group_name = strtoupper($new_table_name . "_AT");
            } elseif ($group_mode == 'MANUAL') {
                $group_name = strtoupper($new_table_name . "_MN");
            }
            $rep_insert_stmt = "insert into CT_REP_GROUPS (group_name, group_mode,  connection_id, created_at, updated_at) values ('" . $group_name . "','" . $group_mode . "','" . $src_id . "', SYSDATE, SYSDATE)";
            DB::connection('mysql2')->insert($rep_insert_stmt);
            $new_rep_group_id = DB::connection('mysql2')->table('CT_REP_GROUPS')->where('group_name', $group_name)->pluck('id')->first();

            foreach ($table_array as $row) {
                //INSERT INTO CT_MAPPINGS ROW BY ROW
                $row = array_map('strtoupper', $row);
                // dd($row);
                $row_string = implode("','", $row);
                // dd($row_string);
                $insert_stmt = "insert into CT_MAPPINGS (table_name, column_name, column_id, column_type, enabled, identity_column, date_range_comparison, source_connection_id, source_schema_name, source_table_name, source_column_name, source_column_type, column_mode, rep_group_id) values ('" . $row_string . "','" . $new_rep_group_id . "')";
                DB::connection('mysql2')->insert($insert_stmt);
            }
            //CT_TAB_CONF(DSS, TESTTABLE, 'CREATE' status, executionid);
            //  $exec = DB::connection('mysql2')->select("exec CT_TAB_CONF(:V_TAB_OWNER, :V_TAB_NAME, :V_ACTION)", array('v_tab_owner' => $table_owner, 'v_tab_name' => $new_table_name, 'v_action' => 'CREATE'));
            //  dd($exec);

            $procedureName = 'CT_TAB_CONF';
            $status = null;
            $exec_out = null;
            $bindings = [
                'V_TAB_OWNER'  => "$table_owner",
                'V_TAB_NAME' => "$new_table_name",
                'V_ACTION' => 'CREATE',
                'V_STATUS_OUT' => [
                    'value' => &$status,
                    'length' => 1000,
                ],
                'V_EXEC_OUT' => [
                    'value' => &$exec_out,
                    'length' => 1000,
                ],
            ];

            $result = DB::connection('mysql2')->executeProcedure($procedureName, $bindings);
            if ($status != 0) {
                $error_details = DB::connection('mysql2')->table('CT_PROCESS_ERROR_LOG')->where('process_id', $exec_out)->pluck('details')->first();
                //  $error_details = DB::connection('mysql2')->select("SELECT ");
                //   dd($status, $error_details);
                ///editTable/{table_name}/{col_count}

                Alert::error("Error $status", $error_details)->autoClose(5000000);
                return redirect()->route('editTable', [$new_table_name, $new_count, $group_mode]);
            } else {
                Alert::success('Table Created successfully', 'Now Proceed to specify its group!');
                return redirect()->route('view_rep_group', [$src_id, $new_table_name]);
            }
            // dd($status, $exec_out);
        }
    }
    public function update_table(Request $request, $table_name, $col_count)
    {
        $table_owner = 'DSS';
        $rows_ids = [];
        $table_rows_ids = DB::connection('mysql2')->select("SELECT ROWIDTOCHAR(ROWID) FROM CT_MAPPINGS WHERE table_name = '$table_name'");
        foreach ($table_rows_ids as $row_id) {
            $array = get_object_vars($row_id);
            $id = array_values($array);
            array_push($rows_ids, $id[0]);
        }
        $rep_group_id = DB::connection('mysql2')->table('CT_MAPPINGS')->where('table_name', $table_name)->pluck('rep_group_id')->first();
        // dd($rep_group_id);

        $table_rows = DB::connection('mysql2')->select("SELECT * FROM CT_MAPPINGS WHERE table_name = '$table_name'");

        $src_id = $table_rows[0]->source_connection_id;
        $col_names = [];
        $type_names = [];
        $id_names = [];
        $rep_names = [];
        $idCol_names = [];
        $comp_names = [];
        $enabled_names = [];
        $src_column_names = [];
        $src_column_types = [];
        $schema_table_names = [];
        $table_columns = [];
        //  dd($count);
        foreach (range(0, $col_count - 1) as $i) {
            if (array_key_exists($i, $table_rows)) {

                array_push($table_columns, "table_name");
                $col_name = "COLUMN_NAME_$i";
                if ($request->$col_name != null && $request->$col_name != "null") {
                    $new_count = $i;
                    array_push($col_names, $request->$col_name);
                    array_push($table_columns, "column_name");
                } elseif ($request->$col_name == null || $request->$col_name == "null") {
                    $new_count = $i;
                    array_push($col_names, $table_rows[$i]->column_name);
                    array_push($table_columns, "column_name");
                }

                $id_name = "COLUMN_ID_$i";
                if ($request->$id_name != null && $request->$id_name != "null") {
                    $new_count = $i;
                    array_push($id_names, $request->$id_name);
                    array_push($table_columns, "column_id");
                } elseif ($request->$id_name == null || $request->$id_name == "null") {
                    $new_count = $i;
                    array_push($id_names, $table_rows[$i]->column_id);
                    array_push($table_columns, "column_id");
                }

                $type_name = "COLUMN_TYPE_$i";
                if ($request->$type_name != null && $request->$type_name != "null") {
                    $new_count = $i;
                    array_push($type_names, $request->$type_name);
                    array_push($table_columns, "column_type");
                } elseif ($request->$type_name == null || $request->$type_name == "null") {
                    $new_count = $i;
                    array_push($type_names, $table_rows[$i]->column_type);
                    array_push($table_columns, "column_type");
                }

                $enabled_name = "ENABLED_$i";
                if ($request->$enabled_name != null && $request->$enabled_name != "null") {

                    array_push($enabled_names, $request->$enabled_name);
                    array_push($table_columns, "enabled");
                } elseif ($request->$enabled_name == null || $request->$enabled_name == "null") {
                    array_push($enabled_names, $table_rows[$i]->enabled);
                    array_push($table_columns, "enabled");
                }

                $idCol_name = "IDENTITY_COLUMN_$i";
                if ($request->$idCol_name != null && $request->$idCol_name != "null") {

                    array_push($idCol_names, $request->$idCol_name);
                    array_push($table_columns, "identity_column");
                } elseif ($request->$idCol_name == null || $request->$idCol_name == "null") {
                    array_push($idCol_names, $table_rows[$i]->identity_column);
                    array_push($table_columns, "identity_column");
                }

                $comp_name = "DATE_RANGE_COMPARISON_$i";
                if ($request->$comp_name != null && $request->$comp_name != "null") {

                    array_push($comp_names, $request->$comp_name);
                    array_push($table_columns, "date_range_comparison");
                } elseif ($request->$comp_name == null || $request->$comp_name == "null") {
                    array_push($comp_names, $table_rows[$i]->date_range_comparison);
                    array_push($table_columns, "date_range_comparison");
                }
                array_push($table_columns, "source_connection_id");
                $schema_table_name = "SCHEMA_TABLE_$i";
                if ($request->$schema_table_name !== null && $request->$schema_table_name !== "null") {
                    $new_count = $i;
                    array_push($schema_table_names, $request->$schema_table_name);
                    array_push($table_columns, "source_schema_name");
                    array_push($table_columns, "source_table_name");
                } elseif ($request->$schema_table_name == null || $request->$schema_table_name == "null") {
                    $new_count = $i;
                    array_push($schema_table_names, $table_rows[$i]->source_schema_name);
                    array_push($schema_table_names, $table_rows[$i]->source_table_name);
                    array_push($table_columns, "source_schema_name");
                    array_push($table_columns, "source_table_name");
                }

                $src_column_name = "SOURCE_COLUMN_NAME_$i";
                if ($request->$src_column_name != null && $request->$src_column_name != "null") {
                    $new_count = $i;
                    array_push($src_column_names, $request->$src_column_name);
                    array_push($table_columns, "source_column_name");
                } elseif ($request->$src_column_name == null || $request->$src_column_name == "null") {
                    $new_count = $i;
                    array_push($src_column_names, $table_rows[$i]->source_column_name);
                    array_push($table_columns, "source_column_name");
                }

                $src_column_type = "SOURCE_COLUMN_TYPE_$i";
                if ($request->$src_column_type != null && $request->$src_column_type != "null") {
                    $new_count = $i;
                    array_push($src_column_types, $request->$src_column_type);
                    array_push($table_columns, "source_column_type");
                } elseif ($request->$src_column_type == null || $request->$src_column_type == "null") {
                    $new_count = $i;
                    array_push($src_column_types, $table_rows[$i]->source_column_type);
                    array_push($table_columns, "source_column_type");
                }

                $repMode_name = "REP_MODE_$i";
                if ($request->$repMode_name != null && $request->$repMode_name != "null") {
                    $new_count = $i;
                    array_push($rep_names, $request->$repMode_name);
                    array_push($table_columns, "column_mode");
                } elseif ($request->$repMode_name == null || $request->$repMode_name == "null") {
                    $new_count = $i;
                    array_push($rep_names, $table_rows[$i]->column_mode);
                    array_push($table_columns, "column_mode");
                }
            }
        }
        //  dd($table_columns,$col_names,$type_names,$id_names,$rep_names,$idCol_names,$comp_names,$enabled_names,$src_column_names,$src_column_types,$schema_table_names);
        $table_array = array();
        $new_table_name = $request->TABLE_NAME ?? $table_name;
        $new_count = $new_count + 1;
        // dd($new_count);
        for ($i = 0; $i < $new_count; $i++) {
            for ($j = 0; $j < $new_count; $j++) {
                if (array_key_exists($i, $col_names) && array_key_exists($i, $type_names) && array_key_exists($i, $id_names) && array_key_exists($i, $rep_names) && array_key_exists($i, $idCol_names) && array_key_exists($i, $comp_names) && array_key_exists($i, $enabled_names) && array_key_exists($i, $src_column_names) && array_key_exists($i, $src_column_types) && array_key_exists($i, $schema_table_names)) {
                    $schema_table = preg_split("/\./", $schema_table_names[$i]);
                    // dd($schema_table[0]);                                                                                                                                        //src_schema,src_table,src_column,src_column_type,rep_mode,rep_group_id
                    $table_array[$i] = [$new_table_name, $col_names[$i], $id_names[$i], $type_names[$i], $enabled_names[$i], $idCol_names[$i], $comp_names[$i], $src_id, $schema_table[0], $schema_table[1], $src_column_names[$i], $src_column_types[$i], $rep_names[$i]];
                }
            }
        }

        //  dd(array_unique($table_columns));
        //  dd($col_count, $new_count,count($table_rows));
        foreach ($table_array as $i => $row) {

            $row = array_map('strtoupper', $row);
            //dd($row);
            $columns_and_values[$i] = array_combine(array_unique($table_columns), $row);
        }
        //  dd($row);
        $row_string = implode("','", $row);
        // // dd($row_string);
        if ($col_count != count($table_rows)) {
            $insert_stmt = "INSERT INTO CT_MAPPINGS (table_name, column_name, column_id, column_type, enabled, identity_column, date_range_comparison, source_connection_id, source_schema_name, source_table_name, source_column_name, source_column_type, column_mode, rep_group_id) values ('" . $row_string . "',$rep_group_id)";
            DB::connection('mysql2')->insert($insert_stmt);
        }

        $updates_statements = [];
        foreach ($columns_and_values as $col_and_val) {
            $update_statement = [];

            foreach ($col_and_val as $key => $value) {

                $update = $value;
                array_push($update_statement, "" . $key . " = '" . $update . "'");
            }
            $update_stmt = implode(', ', $update_statement);
            array_push($updates_statements, $update_stmt);
            //   dd($update_stmt);
            //  DB::connection('mysql2')->statement(DB::raw("UPDATE CT_MAPPINGS SET $update_stmt, updated_at = SYSDATE  WHERE id = $rep_id "));
        }
        foreach ($rows_ids as $i => $id) {
            DB::connection('mysql2')->statement(DB::raw("UPDATE CT_MAPPINGS SET $updates_statements[$i], updated_at = SYSDATE  WHERE ROWIDTOCHAR(ROWID) = '$id'"));
        }

        $procedureName = 'CT_TAB_CONF';
        $status = null;
        $exec_out = null;
        $bindings = [
            'V_TAB_OWNER'  => "$table_owner",
            'V_TAB_NAME' => "$new_table_name",
            'V_ACTION' => 'CREATE',
            'V_STATUS_OUT' => [
                'value' => &$status,
                'length' => 1000,
            ],
            'V_EXEC_OUT' => [
                'value' => &$exec_out,
                'length' => 1000,
            ],
        ];

        $result = DB::connection('mysql2')->executeProcedure($procedureName, $bindings);
        if ($status != 0) {
            $error_details = DB::connection('mysql2')->table('CT_PROCESS_ERROR_LOG')->where('process_id', $exec_out)->pluck('details')->first();
            // DB::select(DB::raw("exec GetInventoryDetail :Param1, :Param2"),[
            //     ':Param1' => $param_1,
            //     ':Param2' => $param_2,
            // ]);
            Alert::error("Error $status", $error_details)->autoClose(5000000);
            return redirect()->route('editTable', [$new_table_name, $new_count]);
        } else {
            Alert::success('Success', 'Table Edited successfully');
            // return redirect()->route('view_createdTables');
            // Alert::success('Table Created successfully', 'Now Proceed to specify its group!');
            return redirect()->route('view_rep_group', [$src_id, $new_table_name]);
        }


        // dd($columns_and_values);
    }
    public function deleteTable($table_name)
    {
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
    }

}
