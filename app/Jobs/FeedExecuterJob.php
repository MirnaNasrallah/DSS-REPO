<?php

namespace App\Jobs;

use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldBeUnique;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\DB;

class FeedExecuterJob implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    /**
     * Create a new job instance.
     *
     * @return void
     */
    public function __construct()
    {
        //
    }

    /**
     * Execute the job.
     *
     * @return void
     */
    public function handle()
    {
        $feed_groups = [];
        $feed_ids = DB::connection('mysql2')->table('CT_FEEDS')->where('enabled', 1)->where('deleted_at', NULL)->pluck('id');
        foreach ($feed_ids as $feed_id) {
           // $fd_name = DB::connection('mysql2')->table('CT_FEEDS')->where('id', $feed_id)->pluck('feed_name');
            // $feed_name = str_replace(str_split('\\/:*?"<>|[]'), '', $fd_name);
            $group = DB::connection('mysql2')->select("SELECT * FROM CT_REP_GROUPS WHERE feed_id = $feed_id AND id > 0 AND id < 9000 ORDER BY id ASC");
            $feed_groups[$feed_id] = $group;
        }
        foreach ($feed_groups as $feed_id => $group) {
            $group_id = $group[$feed_id]->id;
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

            $result = DB::connection('mysql2')->executeProcedure($procedureName, $bindings);
            if ($status == 71) {
                //break;
                continue;
            }
            elseif ($status != 0 && $status != 71) {
                $error_details = DB::connection('mysql2')->table('CT_PROCESS_ERROR_LOG')->where('process_id', $exec_guid)->pluck('details')->first();
               // return response()->json(["status" => "200", "message" => $error_details], 200);
            }
            else{
                $query = $details;
                $test_query = DB::connection('mysql2')->statement(DB::raw("$query"));
                echo $test_query;
               // return response()->json(["status" => "200", "message" => $status], 200);
            }
        }
    }
}
