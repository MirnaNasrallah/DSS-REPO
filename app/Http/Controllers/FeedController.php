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

class FeedController extends Controller
{
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
                $group = DB::connection('oracle')->table('CT_REP_GROUPS')->find($data["checked_$i"]);
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
            return view('Feeds_pages/setGroupPriority', compact('groups_ids', 'groups'));
        }
    }
    public function create_feed(Request $request, $groups_ids_string)
    {
        $groups_ids = explode(', ', $groups_ids_string);
        $data = $request->all();
        $data = array_map('strtoupper', $data);
        $feed_id = 0;

        DB::connection('oracle')->insert("INSERT INTO CT_FEEDS (feed_name, feed_sequence, enabled) values ('" . $data['feed_name'] . "','" . $data['feed_sequence'] . "','" . $data['enabled'] . "') ");
        $feed_id_string = DB::connection('oracle')->table('CT_FEEDS')->latest()->pluck('id')->first();
        $feed_id = str_replace(str_split('\\/:*?"<>|[]'), '', $feed_id_string);
        //dd($feed_id);
        //insert into $dest_table ($dest_data) values ($record_data)
        DB::connection('oracle')->statement("UPDATE CT_REP_GROUPS SET feed_id = $feed_id WHERE id IN ($groups_ids_string)");
        for ($i = 0; $i < count($groups_ids); $i++) {
            $priority = "priority_$i";
            $group_id = $groups_ids[$i];
            DB::connection('oracle')->statement("UPDATE CT_REP_GROUPS SET group_priority_feed = '" . $data[$priority] . "' WHERE id = $group_id");
        }
        Alert::success('Success', 'Feed Created successfully');
        return redirect()->route('view_feeds');
    }
    public function updateFeed(Request $request, $feed_id, $count_groups)
    {
        $record = DB::connection('oracle')->table('CT_FEEDS')->find($feed_id);
        $data = $request->all();
        $data = array_map('strtoupper', $data);
        $count = count($request->all());
        for ($i = 0; $i < $count_groups; $i++) {
            $checked = "checked_$i";
            $new_priority = "new_priority_$i";
            if ($request->$checked == 0) {
                continue;
            } else {

                DB::connection('oracle')->statement(DB::raw("UPDATE CT_REP_GROUPS SET feed_id = $feed_id, group_priority_feed = " . $request->$new_priority . " WHERE id = " . $data["checked_$i"] . " "));
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

        DB::connection('oracle')->statement(DB::raw("UPDATE CT_FEEDS SET $update_stmt, updated_at = SYSDATE  WHERE id = $feed_id "));
        Alert::success('Success', 'Feed Edited successfully');
        return redirect()->route('view_feeds');
    }
    public function deleteFeed($id)
    {
        $date = new DateTime();
        $result = $date->format('Y-m-d H:i:s');
        $result = str_replace(['-', ' ', ':'], "", $result);

        $fd_name = DB::connection('oracle')->table('CT_FEEDS')->where('id', $id)->pluck('feed_name')->first();
        $feed_name = $result . '_' . $fd_name;
        //  DB::connection('oracle')->statement(DB::raw('UPDATE CT_CONNECTIONS SET deleted_at = NOW(), connection_name = "' . $result . '_' . $con_name . '"  WHERE id = "' . $id . '"'));
        $delete_stmt = "deleted_at = SYSDATE, feed_name = '" . $feed_name . "'";
        DB::connection('oracle')->statement(DB::raw('UPDATE CT_FEEDS SET ' . $delete_stmt . ' WHERE id = ' . $id . ' '));
        DB::connection('oracle')->statement(DB::raw("UPDATE CT_REP_GROUPS SET feed_id = NULL, group_priority_feed = NULL WHERE feed_id = " . $id . " "));
        //  DB::connection('oracle')->update("UPDATE CT_CONNECTIONS SET deleted_at = SYSDATE, connection_name = ?, WHERE id = ?",["'$connection_name'","'$id'"]);
        Alert::success('Success', 'Removed successfully');
        return redirect()->route('view_feeds');
    }

    public function feed_executer()
    {

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
            DB::connection('oracle')->insert("INSERT INTO CT_FEED_EXE_FEEDBACK (feed_id, status, feedback, started_executing_at, group_id) VALUES ('$feed_id', 'STARTED','BEGAN EXECUTION',SYSDATE, NULL)");
            echo "Feed $feed_id started at $time \n";
            //  return response()->json(["status" => "200", "message" => $group], 200);
            foreach ($feed_group as $group) {
                $group_name = $group->group_name;
                $group_owner = $group->group_owner;
                $table_name = substr($group_name, 0, -3);
                $owner_table_name = "$group_owner.$table_name";
                $staging_table = $owner_table_name . '_STG';

                $src_id = $group->connection_id;
                if ($src_id == 999) {
                    continue;
                } else {
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
                    // if(!$status) { echo "status is null!";}
                    // echo "status : $status \n";
                    if ($status == 71 || $status == 72) {
                        $error_details = DB::connection('oracle')->table('CT_PROCESS_ERROR_LOG')->where('process_id', $exec_guid)->pluck('details')->first() ?? 'Status 71 or 72 returned';
                        DB::connection('oracle')->insert("INSERT INTO CT_FEED_EXE_FEEDBACK (feed_id, status, feedback, started_executing_at, last_executed_at, group_id) VALUES ('$feed_id','Stopped', 'Group $group_name with $group_id was Skipped', SYSDATE, SYSDATE, $group_id)");
                        DB::connection('oracle')->statement(DB::raw("UPDATE CT_FEED_EXE_FEEDBACK SET status = 'Stopped', feedback = '$error_details', last_executed_at = SYSDATE WHERE feed_id = $feed_id AND group_id IS NULL"));
                        echo "group $group_name failed \n";
                        continue;
                    } elseif ($status != 0 && $status != 71  && $status != 72) {
                        $error_details = DB::connection('oracle')->table('CT_PROCESS_ERROR_LOG')->where('process_id', $exec_guid)->pluck('details')->first() ?? 'Execution Failed';
                        DB::connection('oracle')->insert("INSERT INTO CT_FEED_EXE_FEEDBACK (feed_id, status, feedback, started_executing_at, last_executed_at, group_id) VALUES ('$feed_id','Failed', 'Group $group_name with $group_id Execution Failed', SYSDATE, SYSDATE, $group_id)");
                        DB::connection('oracle')->statement(DB::raw("UPDATE CT_FEED_EXE_FEEDBACK SET status = 'Failed', feedback = '$error_details', last_executed_at = SYSDATE WHERE feed_id = $feed_id AND group_id IS NULL"));
                        echo "group $group_name failed \n";
                        continue;
                        //return response()->json(["status" => "200", "message" => $error_details], 200);
                    } else {

                        $query = $details;
                        // echo "breakpoint1 \n";
                        if ($query && $query != 'SKIPPED') {
                            try {
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
                                DB::connection('oracle')->statement(DB::raw("UPDATE CT_REP_GROUPS SET last_executed_at = SYSDATE WHERE group_name = '$group_name'"));

                                echo "Group $group_name finished at $time \n ";
                            } catch (Exception $e) {
                                DB::connection('oracle')->statement(DB::raw("UPDATE CT_FEED_EXE_FEEDBACK SET status = 'Stopped', feedback = 'Source connection lost', last_executed_at = SYSDATE WHERE feed_id = $feed_id AND group_id IS NULL"));
                                DB::connection('oracle')->insert("INSERT INTO CT_FEED_EXE_FEEDBACK (feed_id, status, feedback, started_executing_at, last_executed_at, group_id) VALUES ('$feed_id', 'Skipped', 'Source connection lost', SYSDATE, SYSDATE, $group_id)");
                                Alert::warning('Warning',"" .$e->getMessage()."")->autoClose(5000000);
                                return redirect()->back();
                            }

                        } else {
                            echo "query was empty in group $group_name \n";
                            DB::connection('oracle')->statement(DB::raw("UPDATE CT_FEED_EXE_FEEDBACK SET status = 'Skipped', feedback = 'query was empty', last_executed_at = SYSDATE WHERE feed_id = $feed_id AND group_id IS NULL"));
                            DB::connection('oracle')->insert("INSERT INTO CT_FEED_EXE_FEEDBACK (feed_id, status, feedback, started_executing_at, last_executed_at, group_id) VALUES ('$feed_id', 'Skipped', 'Group $group_name with id $group_id query was empty', SYSDATE, SYSDATE, $group_id)");
                            continue;
                        }
                    }
                }
            }
            $time_now = new DateTime();
            $time = $time_now->format('Y-m-d H:i:s');
            echo "Executed the Feed $feed_id at $time" . "\n";
            DB::connection('oracle')->statement(DB::raw("UPDATE CT_FEED_EXE_FEEDBACK SET status = 'Complete', feedback = 'Completed successfully', last_executed_at = SYSDATE WHERE feed_id = $feed_id AND group_id IS NULL"));
        }
        return response()->json(["status" => "200", "message" => "All done!"], 200);
    }
}
