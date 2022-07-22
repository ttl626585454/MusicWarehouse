
# 需求分析
## 统计歌曲热度，歌手热度
用户可以点播歌曲，根据用户在过去每日在机器上的点播歌曲
统计出最近1日，7日，30日的歌曲热度和歌手热度  
### 统计指标
```text
1日歌曲热度，歌手热度  
7日歌曲热度，歌手热度  
30日歌曲热度，歌手热度
```
### 分析
需要知道用户每日点歌数据  
需要知道歌手-歌曲信息  
```text
歌曲数据结构
机器编号  &  用户操作  &  当前请求携带的json数据  & 机器版本号 &  UI版本

歌手-歌曲数据结构
歌曲编号    歌曲名称    歌曲备注名称      歌手信息    发行时间    发行专辑
```
在json字符串中存在着歌曲信息  
需要过滤点歌操作，分别不同时间统计歌曲信息  
将歌曲信息表和歌手-歌曲信息表连接   
详细指标
```text
1日歌曲的点唱量
1日歌曲的点赞量
1日歌曲的点唱用户数
1日歌曲的点唱订单数
7日歌曲的点唱量
7日歌曲的点赞量
7日歌曲的最高点唱量
7日歌曲的最高点唱量
7日歌曲的点唱用户数
7日歌曲的点唱订单数
30日歌曲的点唱量
30日歌曲的点赞量
30日歌曲的最高点唱量
30日歌曲的最高点唱量
30日歌曲的点唱用户数
30日歌曲的点唱订单数
```
根据微信指数统计歌曲歌手热度  





# 表命名规范
数据层
TO:ODS层表  
TW:EDS层(DW)层表  
TM:DM层表  

## ODS层
```text
数据源（代表来到数仓数据的类型）  
YCAK:机器生产数据系统A  
YCBK:机器生产数据系统B  
SONG:songdb,歌曲数据库中的数据
CLIENT:客户端日志
```
## TW层
```text
MAC:机器信息  
SONG:歌曲  
USR:用户  
SINGER:歌手
```

## 补充说明信息
```text
INFO:基本信息
D:天
M:月
Y:年
REQ:请求
RSI:影响力指数
```
```text
例子 ODS层中的机器信息表
TO_MAC_INFO
```

# 歌曲和歌手热度统计模型分层设计
先将日志数据中70多种操作格式分门别类的放到HDFS的目录下  
再用sqoop 将mysql 中的song表导入到hdfs中
```text
ODS层
    TO_SONG_INFO_D -- 歌库歌曲表     (mysql song 表)
    TO_CLIENT_SONG_PLAY_OPERATR_REQ_D -- 客户端歌曲播放表 (HDFS中每天的日志数据)
```
```text
EDS层
    TW_SONG_RSI_D -- 歌曲影响力指数统计
    TW_SINGER_RSI_D -- 歌手影响力指数日统计
    TW_SONG_FTUR_D -- 歌曲特征日统计表
    TW_SONG_BASEINFO_D -- 歌曲基本信息日全量表
```
```text
DM层
    TM_SONG_RSI -- 歌曲影响力指数表(mysql)
    TM_SINGER_RSI --歌手影响力指数表（mysql）
```
表结构
```text
TO_SONG_INFO_D -- 歌库歌曲表
NBR             ID              
NAME            歌曲名字        
SOURCE          来源              
ALBYM           所属专辑        
PRDCT           发行公司        
LANG            歌曲语言        
VIDEO_FORMAT    视频风格        
DUR             时长              
SINGER1         歌手1姓名       
SINGER2         歌手2姓名       
SINGER1ID       歌手1ID           
SINGER2ID       歌手ID            
MAC_TIME        加入机器时间      
POST_TIME       发行时间        
PINYIN_FST      歌曲首字母       
PINYIN          歌曲全拼        
SING_TYPE       演唱类型        
ORI_SINGER      原唱歌手        
LYRICIST        填词者             
COMPOSER        作曲者         
BPM_VAL         BPM值            
STAR_LEVEL      星级          
VIDEO_QLTY      视频画质        
VIDEO_MK        视频制作方式      
VIDEO_FTUR      视频画面特征      
LYRIC_FTUR      歌词字母特点      
IMG_QLTY        画质评价        
SUBTITLES_TYPE  字幕类型            
AUDIO_FMT       音频格式                
ORI_SOUND_QLTY  原唱音质        
ORI_TRK         音轨              
ORI_TRK_VOL     原唱音量
ACC_VER         伴唱版本            
ACC_QLTY        伴唱音质        
ACC_TRK_VOL     伴唱音量            
ACC_TRK         伴唱音轨        
WIDTH           视频分辨率W          
HEIGHT          视频分辨率H          
VIDEO_RSVL      视频分辨率           
SONG_VER        编曲版本            
AUTH_CO         授权公司            
STATE           状态              
PRDCT_TYPE      产品类型            
```

```text
TW_SONG_RSI_D -- 歌曲影响力指数统计
PERIOD      周期  
NBR         歌曲编号
NAME        歌曲名
RSI         近期歌曲热度
RSI_RANK    近期歌曲热度排名
DATA_DT     数据日期            分区字段
```

```text
TW_SINGER_RSI_D -- 歌手影响力指数日统计
PERIOD      周期
SINGER_ID   歌手ID
SINGER_NAME 歌手名称
RSI         近期歌手热度
RSI_RANK    近期歌手热度排名
DATA_DT     数据日期            分区字段
```

```text
TW_SONG_FTUR_D -- 歌曲特征日统计表
NBR         歌曲编号
NAME        歌曲名
SOURCE      来源
ALBUM       所属专辑
PRDCT       发行公司
LANG        语言
VIDEO_FORMAT视频风格    
DUR         时长/秒
SINGER1     歌手1
SINGER2     歌手2
SINGER1ID    歌手1ID   
SINGER2ID   歌手2ID
MAC_TIME    加入机器时间
SING_CNT    当日点唱量
SUPP_CNT    当日点赞量
USR_CNT     当日点唱用户数
ORDR_CNT    当日点唱订单数
RCT_7_SING_CNT  近七天点唱量
RCT_7_SUPP_CNT  近七天点赞量
RCT_7_TOP_SING_CNT  近七天最高日点唱量
RCT_7_TOP_SUPP_CNT  近七天最高日点赞量
RCT_7_USR_CNT       近七天点唱用户数
RCT_7_ORDR_CNT      近七天点唱订单数
RCT_30_SING_CNT     近三十天点唱量
RCT_30_SUPP_CNT     近三十天点赞量
RCT_30_TOP_SING_CNT 近三十天最高日点唱量
RCT_30_TOP_SUPP_CNT 近三十天最高日点赞量
RCT_30_USR_CNT      近三十天点唱用户数
RCT_30_ORDR_CNT     近三十天点唱订单数
DATA_DT             数据日期
``` 

