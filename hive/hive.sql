/*在ODS层中，每一个事件文件都对应着一张HIVE外表*/
/*文件是json格式的*/

/*用户点歌事件信息表,来源：gz日志文件*/
use musicwarehouse;
CREATE external table if not exists
`MINIK_CLIENT_SONG_PLAY_OPERATE_REQ`(
    `songid` STRING,
    `mid` bigint,
    `optrate_type` bigint,
    `uid` bigint,
    `consume_type` bigint,
    `play_time` bigint,
    `dur_time` bigint,
    `session_id` bigint,
    `songname` string,
    `pkg_id` bigint,
    `order_id` bigint
)
row format delimited fields terminated by '\t';

/*歌曲信息表,来源：mysql*/
/*歌曲信息ODS*/
CREATE EXTERNAL TABLE `TO_SONG_INFO_D`(
                                          `nbr` string,
                                          `name` string,
                                          `other_name` string,
                                          `source` int,
                                          `album` string,
                                          `prdct` string,
                                          `lang` string,
                                          `video_format` string,
                                          `dur` int,
                                          `singer_info` string,
                                          `post_time` string,
                                          `pinyin_fst` string,
                                          `pinyin` string,
                                          `sing_type` int,
                                          `ori_singer` string,
                                          `lyricist` string,
                                          `composer` string,
                                          `bpm_val` int,
                                          `star_level` int,
                                          `video_qlty` int,
                                          `video_mk` int,
                                          `video_ftur` int,
                                          `lyric_ftur` int,
                                          `img_qlty` int,
                                          `subtitles_type` int,
                                          `audio_fmt` int,
                                          `ori_sound_qlty` int,
                                          `ori_trk` int,
                                          `ori_trk_vol` int,
                                          `acc_ver` int,
                                          `acc_qlty` int,
                                          `acc_trk_vol` int,
                                          `acc_trk` int,
                                          `width` int,
                                          `height` int,
                                          `video_rsvl` int,
                                          `song_ver` int,
                                          `auth_co` string,
                                          `state` int,
                                          `proct_type` string)
    ROW FORMAT DELIMITED  FIELDS TERMINATED BY '\t';

-- 数据统计获取 TW_SONG_FTUR_D 数据
-- 	to_client_song_play_operate_req_d - 客户端歌曲播放表
-- 	TW_SONG_BASEINFO_D - 歌曲基本信息日全量表
--
-- 	=== 当日信息	- TEMP1
select
    songid,
    count(distinct songid) as song_cnt,
    0 as supp_cnt,
    count(distinct uid) as usr_cnt,
    count(distinct order_id) as order_cnt
from to_client_song_play_operate_req
where year = 2022 and month = 1 and day = 1
group by songid;
//然后和歌曲基本信息表 tw_song_baseinfo_d 进行连接 得出单日统计数据表


