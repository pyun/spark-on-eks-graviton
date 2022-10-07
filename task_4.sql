INSERT OVERWRITE TABLE test.x86_arm_test_1_target1
select
distinct
  a.col127 as cot01,
  a.col4 as cot02,
  a.col7 as cot03,
  a.col8 as cot04,
  a.col9 as cot05,
  a.col10 as cot06
  ,cot07
  ,0 as cot08
  ,cot09
  ,cot10
  ,cot11
  ,cot12
  ,cot13
  ,cot14
  ,cot15
  ,cot16
  ,cot17
  ,cot18
  ,cot19
  ,cot20
  ,cot21
  ,cot22
  ,cot23
  ,cot24
  ,cot25
  ,cot26
  ,cot27
  ,cot28
  ,cot29
  ,cot30
  ,cot31
  ,cot32
  ,cot33
  ,cot34
  ,cot35
  ,cot36
  ,cot37
  ,cot38
  ,cot39
  ,cot40
  ,cot41
  ,cot42
  ,cot43
  ,cot44
  ,cot45
  ,cot46
  ,cot47
  ,cot48
  ,cot49
  ,cot50
  ,cot51
  ,cot52
  ,cot53
  ,cot54
  ,cot55
  ,cot56
  ,cot57
  ,cot58
  ,cot59
  ,cot60
  ,cot61
  ,cot62
  ,cot63
  ,cot64
  ,cot65
  ,cot66
  ,cot67
  ,cot68
  ,col115 as cot69
  ,cot70
  ,cot71
  ,cot72
  ,cot73
  ,cot74
  ,cot75
  ,cot76
  ,cot77
  ,cot78
  ,cot79
  ,cot80
  ,cot81
  ,cot82
  ,cot83
  ,cot84
  ,cot85
  ,cot86
  ,cot87
  ,cot88
  ,cot89
  ,cot90
  ,cot91
  ,cot92
  ,cot93
  ,cot94
  ,cot95
  ,cot96
  ,cot97
  ,cot98
  ,cot99
  ,cot100
  ,cot101
  ,cot102
  ,cot103
  ,cot104
  ,cot105
  ,cot106
  ,cot107
  ,cot108
  ,cot109
  ,cot110
  ,cot111
  ,cot112
  ,cot113
  ,cot114
  ,cot115
  ,cot116
  ,cot117
  ,col77 as cot118
  ,col78 as cot119
  ,cot120
  ,col79 as cot121
  ,cot122
  ,cot123
  ,col71 as cot124
  ,col72 as cot125
  ,cot126
  ,cot127
  ,col80 as cot128
  ,cot129
  ,col81 as cot130
  ,cot131
  ,col27 as cot132
  ,cot133
  ,col107 as cot134
from
  (select
    coalesce(col127,'all') as col127
    ,coalesce(col4,'all') as col4
    ,coalesce(col128,'all') as col7
    ,coalesce(col8,'all') as col8
    ,coalesce(col9,'all') as col9
    ,coalesce(col10,'all') as col10
    ,count(distinct col3) as cot07
    ,count(distinct if(col23>0,col3,null)) as cot09
    ,count(distinct if(col23=0 and col24>0,col3,null)) as cot10
    ,count(distinct if(col23=0 and col24=0 and col26+col25>0,col3,null)) as cot11
    ,count(distinct if(col23=0 and col24=0 and col26+col25=0 and col28>0,col3,null)) as cot12
    ,count(distinct if(col23=0 and col24=0 and col26+col25=0 and col28=0,col3,null)) as cot13
    ,count(distinct if(col23>0 and col24=0 and col26+col25=0,col3,null)) as cot14
    ,count(distinct if(col23>0 and col24>0 and col26+col25=0,col3,null)) as cot16
    ,count(distinct if(col23>0 and col24=0 and col26+col25>0,col3,null)) as cot17
    ,count(distinct if(col23>0 and col24>0 and col26+col25>0,col3,null)) as cot19
    ,count(distinct if(col23=0 and col24>0 and col26+col25=0,col3,null)) as cot15
    ,count(distinct if(col23=0 and col24>0 and col26+col25>0,col3,null)) as cot18
    ,count(distinct case when col21 in ('sp','tpcc','tppc','tpuc') then col3 end) as cot20
    ,count(distinct case when col21 in ('tss','tsr') then col3 end) as cot21
    ,count(distinct case when col21 in('ta','tc','tds','tmc','tp','tsx','tsr','tsp','tv','tse','tr','tdd','tm3' ) then col3 end) as cot22
    ,count(distinct case when col21 in ('tj') then col3 end) as cot23
    ,count(distinct case when col21='tpcc' then col3 end) as cot24
    ,count(distinct case when col21='tppc' then col3 end) as cot25
    ,count(distinct case when col21='tpuc' then col3 end) as cot26
    ,count(distinct case when col21='sp' then col3 end) as cot27
    ,count(distinct case when col21='tc' then col3 end) as cot28
    ,count(distinct case when col21='tsr' then col3 end) as cot29
    ,count(distinct case when col21='ta' then col3 end) as cot30
    ,count(distinct case when col21='tds' then col3 end) as cot31
    ,count(distinct case when col21='tmc' then col3 end) as cot32
    ,count(distinct case when col21='tp' then col3 end) as cot33
    ,count(distinct case when col21='tsx' then col3 end) as cot34
    ,count(distinct case when col21='tsp' then col3 end) as cot35
    ,count(distinct case when col21='tv' then col3 end) as cot36
    ,count(distinct case when col21='tse' then col3 end) as cot37
    ,count(distinct case when col21='tr' then col3 end) as cot38
    ,count(distinct case when col21='tdd' then col3 end) as cot39
    ,count(distinct case when col21='tm3' then col3 end) as cot40
    ,count(distinct case when col21='tsr' then col3 end) as cot41
    ,count(distinct case when col21='tss' then col3 end) as cot42
    ,count(distinct case when col21 is null or col21='ot' then col3 end) as fnu
    ,count(distinct case when col17=0 then col3 end) as cot43
    ,count(distinct case when col17>0 and col17<=7 then col3 end) as cot44
    ,count(distinct case when col17>7 and col17<=14 then col3 end) as cot45
    ,count(distinct case when col17>14 and col17<=30 then col3 end) as cot46
    ,count(distinct case when col17>30 and col17<=60 then col3 end) as cot47
    ,count(distinct case when col17>60 and col17<=90 then col3 end) as cot48
    ,count(distinct case when col17>90 and col17<=180 then col3 end) as cot49
    ,count(distinct case when col17>180 then col3 end) as cot50
    ,count(distinct case when col18=0 then col3 end) as cot51
    ,count(distinct case when col18=1 then col3 end) as cot52
    ,count(distinct case when col18=2 then col3 end) as cot53
    ,count(distinct case when col18=3 then col3 end) as cot54
    ,count(distinct case when col18=4 then col3 end) as cot55
    ,count(distinct case when col18=5 then col3 end) as cot56
    ,count(distinct case when col18=6 then col3 end) as cot57
    ,count(distinct case when col18=7 then col3 end) as cot58
    ,count(distinct case when col18>7 and col18<=14 then col3 end) as cot59
    ,count(distinct case when col18>14 and col18<=30 then col3 end) as cot60
    ,count(distinct case when col18>30 and col18<=60 then col3 end) as cot61
    ,count(distinct case when col18>60 and col18<=90 then col3 end) as cot62
    ,count(distinct case when col18>90 and col18<=180 then col3 end) as cot63
    ,count(distinct case when col18>180 then col3 end) as cot64
    ,sum(col23+ col24+col26+col25) as cot65
    ,count(distinct case when col23+ col24+col26+col25>0 then col3 end) as cot66
    ,sum(col20-col19)+count(col19) as cot67
    ,count(distinct case when col20>0 and col19>0 then col3 end ) as cot68
    ,sum(col115) as col115
    ,count(distinct case when col110+coalesce(col42,0)+coalesce(col95,0)+coalesce(col102,0)+coalesce(col103,0)+coalesce(col98,0)+coalesce(col97,0)+coalesce(col99,0)+coalesce(col96,0)+coalesce(col106,0)+coalesce(col101,0)+coalesce(col100,0)+col111+col112+col113+col114>0 then col3 end) as cot70
    ,count(distinct case when col110+coalesce(col39,0)+coalesce(col95,0)+coalesce(col102,0)+coalesce(col103,0)+coalesce(col98,0)+coalesce(col97,0)+coalesce(col99,0)+coalesce(col96,0)+coalesce(col106,0)+coalesce(col101,0)+coalesce(col100,0)+col111+col112+col113+col114>0 then col3 end) as cot71
    ,count(distinct case when coalesce(col42,0)+coalesce(col95,0)+coalesce(col102,0)+coalesce(col103,0)+coalesce(col98,0)+coalesce(col97,0)+coalesce(col99,0)+coalesce(col96,0)+coalesce(col106,0)+coalesce(col101,0)+coalesce(col100,0)+col111+col112+col113+col114>0 then col3 end) as cot72
    ,count(distinct case when coalesce(col39,0)+coalesce(col95,0)+coalesce(col102,0)+coalesce(col103,0)+coalesce(col98,0)+coalesce(col97,0)+coalesce(col99,0)+coalesce(col96,0)+coalesce(col106,0)+coalesce(col101,0)+coalesce(col100,0)+col111+col112+col113+col114>0 then col3 end) as cot73
    ,count(distinct case when col110>0 then col3 end) as cot74
    ,count(distinct case when col42>0 then col3 end) as cot75
    ,count(distinct case when col43>0 then col3 end) as cot76
    ,count(distinct case when col44>0 then col3 end) as cot77
    ,count(distinct case when col39>0 then col3 end) as cot78
    ,sum(col39) as cot84
    ,count(distinct case when col49+col54>0 then col3 end) as cot79
    ,count(distinct case when col45+col50>0 then col3 end) as cot80
    ,count(distinct case when col47+col52>0 then col3 end) as cot81
    ,count(distinct case when col46+col51>0 then col3 end) as cot82
    ,count(distinct case when col48+col53>0 then col3 end) as cot83
    ,count(col49+col54>0) as cot85
    ,count(col45+col50>0) as cot86
    ,count(col47+col52>0) as cot87
    ,count(col46+col51>0) as cot88
    ,count(col48+col53>0) as cot89
    ,count(distinct case when coalesce(col95,0)+coalesce(col102,0)+coalesce(col103,0)+coalesce(col98,0)+coalesce(col97,0)+coalesce(col99,0)+coalesce(col96,0)+coalesce(col106,0)+coalesce(col101,0)+coalesce(col100,0)+col111+col112+col113+col114>0 then col3 end) as cot90
    ,count(distinct case when coalesce(col95,0)>0 then col3 end) as cot91
    ,count(distinct case when coalesce(col102,0)>0 then col3 end) as cot92
    ,count(distinct case when coalesce(col103,0)>0 then col3 end) as cot93
    ,count(distinct case when coalesce(col98,0)+coalesce(col97,0)+coalesce(col99,0)+coalesce(col96,0)+coalesce(col106,0)+coalesce(col101,0)+coalesce(col100,0)+col111+col112+col113+col114>0 then col3 end) as cot94
    ,count(distinct case when coalesce(col98,0)>0 then col3 end) as cot95
    ,count(distinct case when coalesce(col97,0)>0 then col3 end) as cot96
    ,count(distinct case when coalesce(col99,0)>0 then col3 end) as cot97
    ,count(distinct case when coalesce(col96,0)>0 then col3 end) as cot98
    ,count(distinct case when coalesce(col106,0)>0 then col3 end) as cot99
    ,count(distinct case when coalesce(col101,0)>0 then col3 end) as cot100
    ,count(distinct case when coalesce(col100,0)>0 then col3 end) as cot101
    ,count(distinct case when col111>0 then col3 end) as cot102
    ,count(distinct case when col112>0 then col3 end) as cot103
    ,count(distinct case when col113>0 then col3 end)  as cot104
    ,count(distinct case when col114>0 then col3 end) as cot105
    ,count(distinct case when col108>0 then col3 end) as cot106
    ,count(distinct case when col88>0 then col3 end) as cot107
    ,count(distinct case when col87>0 then col3 end) as cot108
    ,count(distinct case when col82>0 then col3 end) as cot109
    ,count(distinct case when col83>0 then col3 end) as cot110
    ,count(distinct case when col84>0 then col3 end) as cot111
    ,count(distinct case when col85>0 then col3 end) as cot112
    ,count(distinct case when col86>0 then col3 end) as cot113
    ,count(distinct case when col89>0 then col3 end) as cot114
    ,count(distinct case when col90>0 then col3 end) as cot115
    ,count(distinct case when col77>0 then col3 end) as cot116
    ,count(distinct case when col78>0 then col3 end) as cot117
    ,sum(col77) as col77
    ,sum(col78) as col78
    ,count(distinct case when col77-col78>0 then col3 end) as cot120
    ,sum(col79) as col79
    ,count(distinct case when col71>0 then col3 end) as cot122
    ,count(distinct case when col72>0 then col3 end) as cot123
    ,sum(col71) as col71
    ,sum(col72) as col72
    ,count(distinct case when col71-col72>0 then col3 end) as cot126
    ,count(distinct case when col80>0 then col3 end) as cot127
    ,sum(coalesce(col80,0)) col80
    ,count(distinct case when col81>0 then col3 end) as cot129
    ,sum(coalesce(col81,0)) col81
    ,count(distinct case when col27>0 then col3 end) as cot131
    ,sum(col27) col27
    ,count(distinct case when col107>0 then col3 end) as cot133
    ,sum(col107) col107
    from test.x86_arm_test_1
    group by cube(col127,col4,col128,col8,col9,col10)
  )a