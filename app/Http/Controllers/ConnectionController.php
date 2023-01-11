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


class ConnectionController extends Controller
{
    public function setEnv(Request $request)
    {
        $data = $request->all();
        $validator = Validator::make(
            $data,
            [
                "DB_CONNECTION_NAME" => "required|string",
                "DB_CONNECTION" => ['required', Rule::in(['MYSQL', 'ORACLE', 'mysql', 'oracle'])],
                "DB_HOST" => "required",
                "DB_PORT" => "required|numeric",
                "DB_USERNAME" => "required|string",
                "DB_PASSWORD" => "nullable|string",
                "DB_DATABASE" => "required|string",
                "DB_SCHEMA_NAME" => "nullable|string",
            ]
        );

        $data['DB_CONNECTION_NAME'] = strtoupper($data['DB_CONNECTION_NAME']);
        $data['DB_CONNECTION'] = strtoupper($data['DB_CONNECTION']);
        // $data['DB_SCHEMA_NAME'] = strtoupper($data['DB_SCHEMA_NAME']);
        //  $data['DB_CONNECTION_NAME'] = array_map('strtoupper', $data['DB_CONNECTION_NAME']);

        if ($validator->fails()) {
            return Redirect::back()->withErrors($validator);
        } else {


            $connection_name_already_found = DB::connection('mysql2')->table('CT_CONNECTIONS')->where('connection_name', $data['DB_CONNECTION_NAME'])->first();
            if ($connection_name_already_found) {
                Alert::error('Error', 'Connection name already used')->autoClose(5000000);
                return Redirect::back();
            } else {
                $con_columns_array = Schema::connection('mysql2')->getColumnListing('CT_CONNECTIONS');

                if (($key = array_search(strtolower('ID'), array_map('strtolower', $con_columns_array))) !== false) {
                    unset($con_columns_array[$key]);
                }
                if (($key = array_search(strtolower('CREATED_AT'), array_map('strtolower', $con_columns_array))) !== false) {

                    unset($con_columns_array[$key]);
                }
                if (($key = array_search(strtolower('UPDATED_AT'), array_map('strtolower', $con_columns_array))) !== false) {
                    unset($con_columns_array[$key]);
                }
                if (($key = array_search(strtolower('DELETED_AT'), array_map('strtolower', $con_columns_array))) !== false) {
                    unset($con_columns_array[$key]);
                }
                if (($key = array_search(strtolower('CONFIGURED'), array_map('strtolower', $con_columns_array))) !== false) {
                    unset($con_columns_array[$key]);
                }
                // dd($con_columns_array);
                $con_columns = implode(', ', $con_columns_array);
                // dd($con_columns);
                $values = [];
                foreach ($data as $key => $value) {
                    //echo $key . "\n";
                    if ($value == null) {
                        array_push($values, 'NULL');
                    } else {
                        // if($key == "DB_PASSWORD")
                        // {
                        //     $hashed = Hash::make($data[$key]);
                        //     array_push($values, "'$hashed'");

                        // }else{
                        array_push($values, "'$value'");
                        //  }

                    }
                }
                $record_data = implode(', ', $values);

                $insert_stmt = "insert into CT_CONNECTIONS ($con_columns) values ($record_data)";
                // $insert_stmt = "insert into $dest_table ($dest_data) values ('" . $candidate_id_src . "','" . $candidate_name_src . "','" . $candidate_created_at_src . "','" . $candidate_updated_at_src . "','" . $candidate_deleted_at_src . "')";
                DB::connection('mysql2')->insert($insert_stmt);

                $connection_name = $data['DB_CONNECTION_NAME'];
                // $driver = $data['DB_CONNECTION'];
                $new_id = DB::connection('mysql2')->table('CT_CONNECTIONS')->where('connection_name', $connection_name)->pluck('id')->first();
                $con = DB::connection('mysql2')->table('CT_CONNECTIONS')->find($new_id);
                $driver = $con->connection_driver;
                $connection_name = $con->connection_name;
                $connection = DatabaseConnection::setConnection($driver, $connection_name, $new_id);
                try {
                    if ($driver == "oracle") {
                        $test_connection = $connection->statement(DB::raw('SELECT 1 FROM DUAL'));
                        //   dd(env('DB_CONNECTION'));
                    } else {
                        $test_connection = $connection->statement(DB::raw('SELECT 1+1'));
                        //    dd($test);
                    }
                    DB::connection('mysql2')->statement(DB::raw('UPDATE CT_CONNECTIONS SET configured = 1 WHERE id = ' . $con->id . ''));
                    Alert::success('Success', 'Connected successfully');
                    return redirect()->route('view_connections');
                } catch (Exception $e) {
                    DB::connection('mysql2')->statement(DB::raw('UPDATE CT_CONNECTIONS SET configured = 0 WHERE id = ' . $con->id . ''));
                    Alert::error('Error', $e->getMessage())->autoClose(5000000);
                    return redirect()->route('editConnection', $new_id);
                }
            }
            // $path = base_path('.env');
            // if (file_exists($path)) {

            //     $string = "\nDB_CONNECTION_$connection_name=" . $data['DB_CONNECTION'] . "\nDB_HOST_$connection_name=" . $data['DB_HOST'] . "\nDB_PORT_$connection_name=" . $data['DB_PORT'] . "\nDB_DATABASE_$connection_name=" . $data['DB_DATABASE'] . "\nDB_USERNAME_$connection_name=" . $data['DB_USERNAME'] . "\nDB_PASSWORD_$connection_name=" . $data['DB_PASSWORD'] . "\n";
            //     file_put_contents($path, $string, FILE_APPEND | LOCK_EX);
            // }


            // echo config(['database.connections.mysql.host' => '127.0.0.0']);
        }
    }
    public function editEnv(Request $request, $id)
    {
        $data = $request->all();
        $validator = Validator::make(
            $data,
            [
                "DB_CONNECTION_NAME" => "nullable|string",
                "DB_CONNECTION" => ['nullable', Rule::in(['mysql', 'oracle'])],
                "DB_HOST" => "nullable",
                "DB_PORT" => "nullable|numeric",

                "DB_USERNAME" => "nullable|string",
                "DB_PASSWORD" => "nullable|string",
                "DB_DATABASE" => "nullable|string",
                "DB_SCHEMA_NAME" => "nullable|string",
            ]
        );
        if ($data['DB_CONNECTION_NAME']) {
            $data['DB_CONNECTION_NAME'] = strtoupper($data['DB_CONNECTION_NAME']);
        }
        if ($data['DB_CONNECTION']) {
            $data['DB_CONNECTION'] = strtoupper($data['DB_CONNECTION']);
        }




        if ($validator->fails()) {
            return Redirect::back()->withErrors($validator);
        } else {
            $old_connection = DB::connection('mysql2')->table('CT_CONNECTIONS')->find($id);
            $old_connection_name = $old_connection->connection_name;
            $driver = $data['DB_CONNECTION'];
            $new_connection_name = $data['DB_CONNECTION_NAME'] ?? $old_connection_name;
            $new_data = array("DB_CONNECTION" => "" . $data['DB_CONNECTION'] . "", "DB_HOST" => "" . $data['DB_HOST'] . "", "DB_PORT" => "" . $data['DB_PORT'] . "", "DB_DATABASE" => "" . $data['DB_DATABASE'] . "", "DB_USERNAME" => "" . $data['DB_USERNAME'] . "", "DB_PASSWORD" => "" . $data['DB_PASSWORD'] . "");
            // $path = base_path('.env');
            // //  echo env('DB_HOST_SECOND');
            // // $path = "C:/Users/UNASRMI/Desktop/test.txt";

            // if (file_exists($path)) {
            //     foreach ($new_data as $name => $value) {
            //         //  dd($value);
            //         if (strpos(file_get_contents($path), $name . "_$old_connection_name") !== false) {
            //             if ($data[$name] == null) {
            //                 continue;
            //             } else {
            //                 $env_name = $name;
            //                 $env_name .= "_$old_connection_name";


            //                 file_put_contents($path, str_replace(
            //                     $env_name . '=' . env($env_name),
            //                     $env_name . '=' . $data[$name],
            //                     file_get_contents($path)
            //                 ));
            //             }
            //         } else {
            //             continue;
            //         }
            //     }

            //     foreach ($new_data as $name => $value) {
            //         if ($new_connection_name) {
            //             file_put_contents($path, str_replace(
            //                 $name . "_$old_connection_name",
            //                 $name . "_$new_connection_name",
            //                 file_get_contents($path)
            //             ));
            //         }
            //     }
            // }
        }
        $record = DB::connection('mysql2')->table('CT_CONNECTIONS')->find($id);
        $con_columns_array = Schema::connection('mysql2')->getColumnListing('CT_CONNECTIONS');
        if (($key = array_search(strtolower('ID'), array_map('strtolower', $con_columns_array))) !== false) {
            unset($con_columns_array[$key]);
        }
        if (($key = array_search(strtolower('CREATED_AT'), array_map('strtolower', $con_columns_array))) !== false) {

            unset($con_columns_array[$key]);
        }
        if (($key = array_search(strtolower('UPDATED_AT'), array_map('strtolower', $con_columns_array))) !== false) {
            unset($con_columns_array[$key]);
        }
        if (($key = array_search(strtolower('DELETED_AT'), array_map('strtolower', $con_columns_array))) !== false) {
            unset($con_columns_array[$key]);
        }
        if (($key = array_search(strtolower('CONFIGURED'), array_map('strtolower', $con_columns_array))) !== false) {
            unset($con_columns_array[$key]);
        }
        $con_columns = implode(', ', $con_columns_array);
        $update_statement = [];
        $values = [];

        foreach ($data as $key => $value) {

            if ($value === null) {

                array_push($values, 'NULL');
                //    }

            } else {

                array_push($values, $value);
            }
        }
        // dd($con_columns_array,$values);
        // dd(values);
        $col_and_data = array_combine($con_columns_array, $values);
        //dd($col_and_data);
        // dd(array_values($col_and_data));
        foreach ($col_and_data as $key => $value) {

            if ($value == "NULL") {
                array_push($update_statement, "" . $key . " = '" . $record->$key . "'");
                //  array_push($update_statement, '' . $key . ' = "' . $record->$key . '"');
            } else {
                // if($key == "CONNECTION_PASSWORD")
                //     {
                //         if($col_and_data[$key] == '')
                //         {

                //             array_push($update_statement,'' . $key . ' = ""');
                //         }else{
                //             $hashed = Hash::make($col_and_data[$key]);
                // array_push($update_statement, '' . $key . ' = "' . $value . '"');
                //        }

                $update = $value;
                array_push($update_statement, "" . $key . " = '" . $update . "'");
                //   array_push($update_statement, '' . $key . ' = ' . $update . '');
            }
            //  }
        }
        $update_stmt = implode(', ', $update_statement);
        //mysql now ==> database date
        //  $now = Carbon::now();
        //if oracle change the update stmt with sysdate
        DB::connection('mysql2')->statement(DB::raw('UPDATE CT_CONNECTIONS SET ' . $update_stmt . ',updated_at = SYSDATE  WHERE id = ' . $id . ' '));

        $con = DB::connection('mysql2')->table('CT_CONNECTIONS')->find($id);
        $driver = $con->connection_driver;
        $connection_name = $con->connection_name;

        $connection = DatabaseConnection::setConnection($driver, $connection_name, $id);
        try {
            if ($driver == "oracle") {
                $test_connection = $connection->statement(DB::raw('SELECT 1 FROM DUAL'));
                //   dd(env('DB_CONNECTION'));
            } else {
                $test_connection = $connection->statement(DB::raw('SELECT 1+1'));
                //    dd($test);
            }
            DB::connection('mysql2')->statement(DB::raw('UPDATE CT_CONNECTIONS SET configured = 1 WHERE id = ' . $con->id . ''));
            Alert::success('Success', 'Connected successfully');
            return redirect()->route('view_connections');
        } catch (Exception $e) {
            DB::connection('mysql2')->statement(DB::raw('UPDATE CT_CONNECTIONS SET configured = 0 WHERE id = ' . $con->id . ''));
            Alert::error('Error', $e->getMessage())->autoClose(5000000);
            return redirect()->route('editConnection', $id);
        }
    }
    public function deleteConnection($id)
    {
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
    }
}
