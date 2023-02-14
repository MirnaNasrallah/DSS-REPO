<?php

namespace App\Jobs;

use App\Helpers\DatabaseConnection;
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
    public $feed_groups;
    /**
     * Create a new job instance.
     *
     * @return void
     */
    public function __construct(array $feed_groups)
    {
        $this->feed_groups = $feed_groups;
    }

    /**
     * Execute the job.
     *
     * @return void
     */
    public function handle()
    {
        $feed_groups = [];
        $feed_ids = DB::connection('oracle')->table('CT_FEEDS')->where('enabled', 1)->where('deleted_at', NULL)->orderBy('feed_sequence','asc')->pluck('id');
        $feed_id_grp = $feed_ids[0];
        //  $src_id = DB::connection('oracle')->select("SELECT connection_id FROM CT_REP_GROUPS WHERE feed_id = $feed_id_grp AND deleted_at = NULL AND enabled = 1");

        foreach ($feed_ids as $feed_id) {
            // $fd_name = DB::connection('oracle')->table('CT_FEEDS')->where('id', $feed_id)->pluck('feed_name');
            // $feed_name = str_replace(str_split('\\/:*?"<>|[]'), '', $fd_name);
            $group = DB::connection('oracle')->select("SELECT * FROM CT_REP_GROUPS WHERE feed_id = $feed_id AND group_priority_feed > 0 AND group_priority_feed < 9000 AND deleted_at IS NULL AND enabled = 1 ORDER BY id, group_priority_feed ASC");
            $feed_groups[$feed_id] = $group;
        }
        //dispatch to group job with atributes (feed_groups)
        foreach ($feed_groups as $feed_id => $group) {
            $src_id = $group[$feed_id]->connection_id;
            $source_connection = DB::connection('oracle')->table('CT_CONNECTIONS')->find($src_id);
            $driver = $source_connection->connection_driver;
            $connection_name = $source_connection->connection_name;
            $src_connection = DatabaseConnection::setConnection($driver, $connection_name, $src_id);

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

            $result = DB::connection('oracle')->executeProcedure($procedureName, $bindings);
            if ($status == 71 || $status == 72) {
                continue;
            } elseif ($status != 0 && $status != 71  && $status != 72) {
                $error_details = DB::connection('oracle')->table('CT_PROCESS_ERROR_LOG')->where('process_id', $exec_guid)->pluck('details')->first();
                //continue;
              //  return response()->json(["status" => "200", "message" => $error_details], 200);
            } else {
                $query = $details;
                // $test_query = $src_connection->statement(DB::raw("$query"));
                $test_query = $src_connection->statement(DB::raw("SELECT * FROM CANDIDATE_SESSIONS"));
                echo $test_query . "\n";
              //  return response()->json(["status" => "200", "message" => $status], 200);
            }

        }
    }
}
