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
            DB::connection('mysql2')->statement("UPDATE CT_REP_GROUPS SET group_priority_feed = '" . $data[$priority] . "' WHERE id = $group_id");
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
    public function deleteFeed($id)
    {
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
    }
}
