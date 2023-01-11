<?php

namespace App\Http\Controllers;


use App\Helpers\DatabaseConnection;
use RealRashid\SweetAlert\Facades\Alert;

use Carbon\Carbon;
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

class TestController extends Controller
{
    public function test2()
    {
        $mapping = DB::connection('mysql2')->select('select * from mapping');
        foreach ($mapping as $map) {
            $now = Carbon::now();
            $start_time = Carbon::now();
            if ($map->status == true) {
                $source_connection = DB::connection('mysql2')->table('CONNECTIONS_MAPPING')->find($map->src_connection_id);
                $driver = $source_connection->CONNECTION_DRIVER;
                $connection_name = $source_connection->CONNECTION_NAME;
                $src_connection = DatabaseConnection::setConnection($driver, $connection_name, $map->src_connection_id);
                $src_schema = DatabaseConnection::getSchema($driver, $connection_name, $map->src_connection_id);
                // $src_connection = $map->src_connection_name;
                $dest_connection = $map->dest_connection_name;
                $src_table = $map->src_table_name;
                $dest_table = $map->dest_table_name;
                $src_columns = $map->src_table_columns;
                $dest_columns = $map->dest_table_columns;
                $columns_highlighted = $map->columns_highlighted;
                $columns_dropped = $map->columns_dropped;
                // WE'LL GET THEM REQUESTED AND STORED NOT FETCHED FROM THE TABLE
                $additional_query = $map->query;
                $query_connection = $map->query_connection;
                $query_condition = $map->query_condition;
                ############################################
                $last_update = $map->last_update;
                $priority = $map->priority;
                $batch_number = $map->batch_number;

                //EXTRA QUERY RUNNING################
                if ($additional_query !== null && $query_connection !== null) {
                    try {
                        if ($query_condition !== null) {
                            DB::connection($query_connection)->statement(DB::raw('' . $additional_query . ' ' . $query_condition . ''));
                        } else {
                            DB::connection($query_connection)->statement(DB::raw('' . $additional_query . ''));
                        }
                    } catch (Exception $e) {
                        echo "There has been a syntax error:" . "\n" . $e->getMessage();
                    }
                } else {
                    if ($additional_query !== null && $query_connection === null) {
                        echo $additional_query ?? $query_connection ?? "All" . " was null \n";
                    }
                }
                //CHECK IF COL NAMES IS NULL: GET THE ORIGINAL ONES
                //IF COLUMNS HIGHLIGHTED != NULL -> MAKE THEM OUR LIST OF COLUMNS
                //IF COLUMNS DROPPED != NULL -> EXCLUDE THEM FROMT HE ARRAY OF COL NAMES
                if ($src_columns === null) {
                    if ($columns_highlighted !== null) {
                        $src_data = $columns_highlighted;
                        $src_data_array = explode(',', $src_data);
                        $src_num_columns = count($src_data_array);
                    } elseif ($columns_dropped !== null) {
                        $columns_dropped_array = explode(',', $columns_dropped);
                        $src_data_array = $src_schema->getColumnListing($src_table);
                        foreach ($columns_dropped_array as $col) {
                            if (($key = array_search($col, $src_data_array)) !== false) {
                                unset($src_data_array[$key]);
                            }
                        }
                        $src_data = implode(', ', $src_data_array);
                    } else {
                        $src_data_array = $src_schema->getColumnListing($src_table);
                        $src_num_columns = count($src_data_array);
                        $src_data = implode(',', $src_data_array);
                    }
                } else {
                    if ($columns_highlighted !== null) {
                        $src_data = $columns_highlighted;
                        $src_data_array = explode(',', $src_data);
                        $src_num_columns = count($src_data_array);
                    } elseif ($columns_dropped !== null) {
                        $columns_dropped_array = explode(',', $columns_dropped);
                        $src_data = $src_columns;
                        $src_data_array = explode(',', $src_columns);
                        foreach ($columns_dropped_array as $col) {
                            if (($key = array_search($col, $src_data_array)) !== false) {
                                unset($src_data_array[$key]);
                            }
                        }
                        $src_data = implode(', ', $src_data_array);
                    } else {
                        $src_data_array = $src_schema->getColumnListing($src_table);
                        $src_num_columns = count($src_data_array);
                        $src_data = implode(',', $src_data_array);
                    }
                }

                if ($dest_columns === null) {
                    if ($columns_highlighted !== null) {
                        $dest_data = $columns_highlighted;
                        $dest_data_array = explode(',', $dest_data);
                        $dest_num_columns = count($dest_data_array);
                    } elseif ($columns_dropped !== null) {
                        $columns_dropped_array = explode(',', $columns_dropped);
                        $dest_data_array = Schema::connection($dest_connection)->getColumnListing($dest_table);
                        foreach ($columns_dropped_array as $col) {
                            if (($key = array_search($col, $dest_data_array)) !== false) {
                                unset($dest_data_array[$key]);
                            }
                        }
                        $dest_data = implode(',', $dest_data_array);
                    } else {
                        $dest_data_array = Schema::connection($dest_connection)->getColumnListing($dest_table);
                        $dest_num_columns = count($dest_data_array);
                        $dest_data = implode(',', $dest_data_array);
                    }
                } else {
                    if ($columns_highlighted !== null) {
                        $dest_data = $columns_highlighted;
                        $dest_data_array = explode(',', $dest_data);
                        $dest_num_columns = count($dest_data_array);
                    } elseif ($columns_dropped !== null) {
                        $columns_dropped_array = explode(',', $columns_dropped);
                        $dest_data = $dest_columns;
                        $dest_data_array = explode(',', $dest_columns);
                        foreach ($columns_dropped_array as $col) {
                            if (($key = array_search($col, $dest_data_array)) !== false) {
                                unset($dest_data_array[$key]);
                            }
                        }
                        $dest_data = implode(',', $dest_data_array);
                    } else {
                        $dest_data_array = Schema::connection($dest_connection)->getColumnListing($dest_table);
                        $dest_num_columns = count($dest_data_array);
                        $dest_data = implode(',', $dest_data_array);
                    }
                }

                $dataAreIdentical = ($src_data_array == $dest_data_array);
                if ($dataAreIdentical) {

                    if ($batch_number != 0) {
                        if ($last_update !== null) {
                            $src = $src_connection->select('select * from ' . $src_table . '  where updated_at > "' . $last_update . '" order by updated_at asc limit ' . $batch_number . '');
                            $dest = DB::connection($dest_connection)->select('select * from ' . $dest_table . '');
                        } else {
                            $src = $src_connection->select('select * from ' . $src_table . ' order by id asc limit ' . $batch_number . '');

                            $dest = DB::connection($dest_connection)->select('select * from ' . $dest_table . '');
                        }
                    } else {
                        $src = $src_connection->select('select * from ' . $src_table . '');
                        $dest = DB::connection($dest_connection)->select('select * from ' . $dest_table . '');
                    }

                    // echo "\n".$src_data."\n".$dest_data;
                    if ($src) {
                        // $max_updated_at_dest = DB::connection($dest_connection)->statement(DB::raw('SELECT max(updated_at) FROM '.$dest_table.''));
                        // $max_updated_at_src = DB::connection($src_connection)->statement(DB::raw('SELECT max(updated_at) FROM '.$src_table.''));

                        $max_updated_at_dest = DB::connection($dest_connection)->table($dest_table)->max('updated_at');
                        $max_updated_at_src = $src_connection->table($src_table)->max('updated_at');

                        //    echo $max_updated_at_dest ." ". $max_updated_at_src ."\n";
                        if ($max_updated_at_src > $max_updated_at_dest) {
                            //echo "I am executing update \n";
                            // $updated_ids = DB::connection($src_connection)->table($src_table)->select('*')->where('updated_at', '<=', $max_updated_at_src)->where('updated_at', '>', $max_updated_at_dest)->pluck('id');
                            // $updated_records = DB::connection($src_connection)->table($src_table)->select('*')->where('updated_at', '<=', $max_updated_at_src)->where('updated_at', '>', $max_updated_at_dest);
                            if ($batch_number != 0) {
                                $updated_records = $src_connection->select('select * from ' . $src_table . ' where updated_at <= "' . $max_updated_at_src . '" and updated_at > "' . $max_updated_at_dest . '" limit ' . $batch_number . '');
                            } else {
                                $updated_records = $src_connection->select('select * from ' . $src_table . ' where updated_at <= "' . $max_updated_at_src . '" and updated_at > "' . $max_updated_at_dest . '"');
                            }

                            $update_statement = [];
                            foreach ($updated_records as $record) {
                                foreach ($src_data_array as $key) {

                                    if ($record->$key === null) {

                                        array_push($update_statement, '' . $key . ' = NULL');
                                    } else {

                                        array_push($update_statement, '' . $key . ' = "' . $record->$key . '"');
                                    }
                                }
                                $update_stmt = implode(', ', $update_statement);

                                echo "I am executing update \n";
                                //echo $id;
                                DB::connection($dest_connection)->statement(DB::raw('update ' . $dest_table . ' set ' . $update_stmt . ' where ' . $dest_data_array[0] . ' = "' . $record->id . '"'));
                            }
                        }
                        foreach ($src as $record) {
                            $record_last_update = $record->updated_at;
                            $src_value = [];

                            $update_statement = [];

                            foreach ($src_data_array as $key) {


                                if ($record->$key === null) {
                                    array_push($src_value, 'NULL');
                                    array_push($update_statement, '' . $key . ' = NULL');
                                } else {
                                    $data = $record->$key;
                                    array_push($src_value, "'$data'");
                                    array_push($update_statement, '' . $key . ' = "' . $record->$key . '"');
                                }
                            }
                            $record_data = implode(', ', $src_value);
                            $update_stmt = implode(', ', $update_statement);
                            // echo $record_data, "\n";
                            $id_src = $record->id;
                            $deleted_at_src = $record->deleted_at;

                            $dest_record = DB::connection($dest_connection)->table($dest_table)->find($id_src);

                            //  echo $record->$src_data['id'];
                            if ($dest_record === null) {

                                $insert_stmt = "insert into $dest_table ($dest_data) values ($record_data)";
                                // $insert_stmt = "insert into $dest_table ($dest_data) values ('" . $candidate_id_src . "','" . $candidate_name_src . "','" . $candidate_created_at_src . "','" . $candidate_updated_at_src . "','" . $candidate_deleted_at_src . "')";
                                DB::connection($dest_connection)->insert($insert_stmt);
                            }


                            if ($dest_record !== null && $deleted_at_src !== null) {
                                //echo $candidate_deleted_at_src;
                                DB::connection($dest_connection)->statement(DB::raw('update ' . $dest_table . ' set deleted_at = "' . $deleted_at_src . '" where ' . $dest_data_array[0] . ' = "' . $id_src . '"'));
                                //DB::connection('mysql2')->update("update candidates set deleted_at = '$candidate_deleted_at_src' where id = ?", [$candidate_id_src]);
                            } elseif ($dest_record !== null && $deleted_at_src === null) {
                                DB::connection($dest_connection)->statement(DB::raw('update ' . $dest_table . ' set deleted_at = null where ' . $dest_data_array[0] . ' = "' . $id_src . '"'));
                            }
                        }
                        DB::connection('mysql2')->statement(DB::raw('UPDATE mapping SET last_update = "' . $record_last_update . '" where src_table_name = "' . $src_table . '"'));
                    } else {
                        foreach ($dest as $record) {
                            //   $record->delete();
                        }
                    }
                } else {
                    $now = Carbon::now();
                    $op = strtotime($now);
                    if (Schema::connection($dest_connection)->hasTable('new_' . $src_table . '_' . $op . '')) {
                        echo "table has already been created";
                    } else {
                        $new_table_stmt = [];

                        foreach ($src_data_array as $col_name) {
                            $type = $src_connection->getDoctrineColumn($src_table, $col_name)->getType()->getName();
                            $size = $src_connection->getDoctrineColumn($src_table, $col_name)->getLength();
                            if ($size === null) {
                                if ($type == "string") {
                                    array_push($new_table_stmt, '' . $col_name . ' varchar');
                                } else {
                                    array_push($new_table_stmt, '' . $col_name . ' ' . $type . '');
                                }
                            } else {
                                if ($type == "string") {
                                    array_push($new_table_stmt, '' . $col_name . ' varchar(' . $size . ')');
                                } else {
                                    array_push($new_table_stmt, '' . $col_name . ' ' . $type . '(' . $size . ')');
                                }
                            }
                        }
                        $create_stmt = implode(',', $new_table_stmt);
                        $create_table = 'CREATE TABLE new_' . $src_table . '_' . $op . '(' . $create_stmt . ',PRIMARY KEY (id))';
                        DB::connection($dest_connection)->statement($create_table);

                        $dest_table = 'new_' . $src_table . '_' . $op . '';
                        DB::connection('mysql2')->statement(DB::raw('UPDATE mapping SET dest_table_name = "' . $dest_table . '" where src_table_name = "' . $src_table . '"'));
                        $dest_data_array = Schema::connection($dest_connection)->getColumnListing($dest_table);
                        $dest_data = implode(',', $dest_data_array);

                        $src = $src_connection->select('select * from ' . $src_table . '');

                        $dest = DB::connection($dest_connection)->select('select * from ' . $dest_table . '');

                        if ($src) {

                            foreach ($src as $record) {
                                $src_value = [];

                                foreach ($src_data_array as $key) {


                                    if ($record->$key === null) {
                                        array_push($src_value, 'NULL');
                                        array_push($update_statement, '' . $key . ' = NULL');
                                    } else {
                                        $data = $record->$key;
                                        array_push($src_value, "'$data'");
                                        array_push($update_statement, '' . $key . ' = "' . $record->$key . '"');
                                    }
                                }
                                $record_data = implode(', ', $src_value);
                                $update_stmt = implode(', ', $update_statement);
                                echo $record_data, "\n";
                                $candidate_id_src = $record->id;
                                $candidate_updated_at_src = $record->updated_at;
                                $candidate_deleted_at_src = $record->deleted_at;

                                $dest_record = DB::connection($dest_connection)->table($dest_table)->find($candidate_id_src);
                                //  echo $record->$src_data['id'];
                                if ($dest_record === null) {
                                    $insert_stmt = "insert into $dest_table ($dest_data) values ($record_data)";
                                    // $insert_stmt = "insert into $dest_table ($dest_data) values ('" . $candidate_id_src . "','" . $candidate_name_src . "','" . $candidate_created_at_src . "','" . $candidate_updated_at_src . "','" . $candidate_deleted_at_src . "')";
                                    DB::connection($dest_connection)->insert($insert_stmt);
                                }
                                if ($dest_record !== null && $candidate_updated_at_src != $dest_record->updated_at) {
                                    // echo "I am executing update \n";
                                    DB::connection($dest_connection)->statement(DB::raw('update ' . $dest_table . ' set ' . $update_stmt . ' where ' . $dest_data_array[0] . ' = "' . $candidate_id_src . '"'));
                                }
                                if ($dest_record !== null && $candidate_deleted_at_src !== null) {
                                    //echo $candidate_deleted_at_src;
                                    DB::connection($dest_connection)->statement(DB::raw('update ' . $dest_table . ' set deleted_at = "' . $candidate_deleted_at_src . '" where ' . $dest_data_array[0] . ' = "' . $candidate_id_src . '"'));
                                    //DB::connection('mysql2')->update("update candidates set deleted_at = '$candidate_deleted_at_src' where id = ?", [$candidate_id_src]);
                                } elseif ($dest_record !== null && $candidate_deleted_at_src === null) {
                                    DB::connection($dest_connection)->statement(DB::raw('update ' . $dest_table . ' set deleted_at = null where ' . $dest_data_array[0] . ' = "' . $candidate_id_src . '"'));
                                }
                            }
                            //
                        } else {
                            foreach ($dest as $record) {
                                //   $record->delete();
                            }
                        }
                        //   DB::connection($dest_connection)->statement(DB::raw('UPDATE mapping SET dest_table_name = ' . $dest_table . ' where src_table_name = "' . $src_table . '"'));
                    }
                }
                //
            }


            $now = Carbon::now();
            $end_time = Carbon::now();
            $job_def_columns_array = Schema::connection('mysql2')->getColumnListing('JOB_DEF');

            if (($key = array_search('ID',  $job_def_columns_array)) !== false) {
                unset($job_def_columns_array[$key]);
            }
            if (($key = array_search('CREATED_AT',  $job_def_columns_array)) !== false) {
                unset($job_def_columns_array[$key]);
            }
            if (($key = array_search('UPDATED_AT',  $job_def_columns_array)) !== false) {
                unset($job_def_columns_array[$key]);
            }
            if (($key = array_search('DELETED_AT',  $job_def_columns_array)) !== false) {
                unset($job_def_columns_array[$key]);
            }

            $job_def_columns = implode(', ', $job_def_columns_array);
            //  $insert_stmt = "insert into JOB_DEF ($job_def_columns) values ('job_name', '" . $map->status . "', 'completed', '" . $start_time . "', '" . $end_time . "')";

            //     DB::connection($dest_connection)->insert($insert_stmt);
        }
    }
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
            'V_ACTION' => 'MODIFY',
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
                "ENABLED" => "boolean|nullable",
                "QUERY_CONDITION" =>  "nullable|string",

            ]
        );
        $data = array_map('strtoupper', $data);
        $table = DB::connection('mysql2')->table('CT_MAPPINGS')->where('table_name', $new_table_name)->first();

        if ($validator->fails()) {
            Alert::warning('Warning', 'There has been errors in the input data')->autoClose(5000000);;
            return Redirect::back()->withErrors($validator);
        } else {
            if ($dest_driver == 'mysql') {
                DB::connection('mysql2')->statement(DB::raw("UPDATE CT_REP_GROUPS SET enabled = '" . $data['ENABLED'] . "', frequency_minutes= '" . $data['FREQUENCY_MINUTES'] . "',batch_limit= '" . $data['BATCH_LIMIT'] . "',query_condition= '" . $data['QUERY_CONDITION'] . "', updated_at = NOW()  WHERE id = '" . $table->rep_group_id . "' "));

            } else {
                // dd($data['REP_MODE']);
                $update_statement = "enabled = '" . $data['ENABLED'] . "', frequency_minutes= '" . $data['FREQUENCY_MINUTES'] . "',batch_limit= '" . $data['BATCH_LIMIT'] . "',query_condition= '" . $data['QUERY_CONDITION'] . "', updated_at = SYSDATE";
                DB::connection('mysql2')->statement(DB::raw("UPDATE CT_REP_GROUPS SET $update_statement WHERE id = '" . $table->rep_group_id . "' "));

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

        DB::connection('mysql2')->statement(DB::raw("UPDATE CT_REP_GROUPS SET $update_stmt, updated_at = SYSDATE  WHERE id = $rep_id "));
        Alert::success('Success', 'REP_GROUP Edited successfully');
        return redirect()->route('view_repGroups');
    }
    public function addToFeed(Request $request)
    {
        $flag = 'true';
        $data = $request->all();
        // $data = array_map('strtoupper', $data);
        $groups_ids = [];
        $groups = [];
        $count = count($request->all());
        for ($i = 0; $i < $count; $i++) {
            if ($data["checked_$i"] == 0) {
                continue;
            } else {
                array_push($groups_ids, $data["checked_$i"]);
                $group = DB::connection('mysql2')->table('CT_REP_GROUPS')->find($data["checked_$i"]);
                array_push($groups, $group);
            }
        }
        foreach ($groups as $group) {
            if ($group->enabled == 0) {
                $flag = 'false';
            }
        }
        if (!$groups_ids || !$groups) {
            Alert::warning('Warning', 'Please Choose one or more groups')->autoClose(100000);
            return redirect()->back();
        } elseif ($flag == 'false') {
            Alert::warning('Warning', 'Please Choose enabled groups')->autoClose(100000);
            return redirect()->back();
        } else {
            return view('setGroupPriority', compact('groups_ids', 'groups'));
        }
    }
    public function create_feed(Request $request, $groups_ids_string)
    {
        $groups_ids = explode(', ', $groups_ids_string);
        $data = $request->all();
        $data = array_map('strtoupper', $data);
        $feed_id = 0;

        DB::connection('mysql2')->insert("INSERT INTO CT_FEEDS (feed_name, feed_sequence, enabled) values ('" . $data['feed_name'] . "','" . $data['feed_sequence'] . "','" . $data['enabled'] . "') ");
        $feed_id_string = DB::connection('mysql2')->table('CT_FEEDS')->latest()->pluck('id')->first();
        $feed_id = str_replace(str_split('\\/:*?"<>|[]'), '', $feed_id_string);
        //dd($feed_id);
        //insert into $dest_table ($dest_data) values ($record_data)
        DB::connection('mysql2')->statement("UPDATE CT_REP_GROUPS SET feed_id = $feed_id WHERE id IN ($groups_ids_string)");
        for ($i = 0; $i < count($groups_ids); $i++) {
            $priority = "priority_$i";
            $group_id = $groups_ids[$i];
            $group = DB::connection('mysql2')->table('CT_REP_GROUPS')->find($group_id);
            if($group->group_mode == 'FULL_FEED')
            {
                DB::connection('mysql2')->statement("UPDATE CT_REP_GROUPS SET group_priority_feed = 0 WHERE id = $group_id");
            }else{
                DB::connection('mysql2')->statement("UPDATE CT_REP_GROUPS SET group_priority_feed = '" . $data[$priority] . "' WHERE id = $group_id");
            }

        }
        Alert::success('Success', 'Feed Created successfully');
        return redirect()->route('view_feeds');
    }
    public function updateFeed(Request $request, $feed_id, $count_groups)
    {
        $record = DB::connection('mysql2')->table('CT_FEEDS')->find($feed_id);
        $data = $request->all();
        $data = array_map('strtoupper', $data);
        $count = count($request->all());
        for ($i = 0; $i < $count_groups; $i++) {
            $checked = "checked_$i";
            $new_priority = "new_priority_$i";
            if ($request->$checked == 0) {
                continue;
            } else {

                DB::connection('mysql2')->statement(DB::raw("UPDATE CT_REP_GROUPS SET feed_id = $feed_id, group_priority_feed = " . $request->$new_priority . " WHERE id = " . $data["checked_$i"] . " "));
            }
        }
        $Feed_data = ["feed_name" => $request->feed_name, "feed_sequence" => $request->feed_sequence, "enabled" => $request->enabled];

        // dd($Feed_data);
        $update_statement = [];
        $columns = [];
        $values = [];

        foreach ($Feed_data as $key => $value) {
            // dd($key);
            if ($value === null) {
                array_push($columns, $key);
                array_push($values, 'NULL');
                //    }

            } else {
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

        DB::connection('mysql2')->statement(DB::raw("UPDATE CT_FEEDS SET $update_stmt, updated_at = SYSDATE  WHERE id = $feed_id "));
        Alert::success('Success', 'Feed Edited successfully');
        return redirect()->route('view_feeds');
    }
    public function feedJob()
    {
        $feed_ids = DB::connection('mysql2')->table('CT_FEEDS')->where('enabled', 1)->where('deleted_at', '!=', NULL)->pluck('feed_id');

    }
}
