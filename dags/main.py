from airflow import DAG
import pendulum
from datetime import timedelta, datetime
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

local_tz = pendulum.timezone("Asia/Kolkata")

defult_args = {
    "owner": "anishformal",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "anish.formal@gmail.com",
    "retries": 1,
    "retry_delta": timedelta(minutes=1),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
    # "end_date" : datetime(2030, 1, 1, tzinfo=local_tz),
}

with DAG(
    dag_id='produce_json',
    default_args=defult_args,
    description='DAG to produce JSON file with raw data',
    schedule='0 14 * * *',
    catchup=False
) as dag:

    #Define task
    playList_id = get_playlist_id()
    video_id = get_video_ids(playList_id)
    extract_data = extract_video_data(video_id)
    save_to_json_task = save_to_json(extract_data)


    #Define Dependinces
    playList_id >> video_id >> extract_data >> save_to_json_task

