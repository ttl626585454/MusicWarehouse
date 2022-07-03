sqoop import \
--connect  jdbc:mysql://192.168.247.129:3306/songDB?dontTrackOpenResources=true\&defaultFetchSize=10000\&useCursorFetch=true\&useUnicode=yes\&characterEncoding=utf8 \
--username root \
--password root \
--table song \
--target-dir /user/hive/warehouse/musicwarehouse.db/to_song_info_d  \
--delete-target-dir \
--num-mappers 1 \
--fields-terminated-by '\t'
