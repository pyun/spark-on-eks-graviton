package mydemo.spark;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDateTime;
import java.util.ArrayList;

public class SparkArmTest {
    public static void main(String[] args) {
        System.out.println("jre version:" + System.getProperty("java.version"));
        if (args.length < 2) return;
        String taskType = args[0];
        String arch = args[1];
        int rePartitions = 0;
        if (args.length > 2) {
            try {
                rePartitions = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }

        testSparkTask(taskType, arch, rePartitions);

    }

    public static void testSparkTask(String taskType, String arch, int rePartitions) {
        System.out.println("1.--------this task is:" + taskType);
        System.out.println("  --------platform:" + arch);
        System.out.println("  --------rePartitions:" + rePartitions);
        SparkSession spark = SparkSession
                .builder()
                .appName("spark_on_eks_" + taskType + "_" + arch)
                //.config("spark.sql.shuffle.partitions", shufflePartitions)
                .getOrCreate();
        try {
            System.out.println("2.--------start prepare data");

            JavaRDD<Row> source = spark.read()
                    .text("s3a://pyunspark/x86_arm_test_1")
                    .javaRDD();
            JavaRDD<Row> rowRDD = schemaRDD(source);

            Dataset<Row> df = spark.createDataFrame(rowRDD, generateSchema(rowRDD));
            if (rePartitions > 0) {
                df = df.repartition(rePartitions);
            }
            df.createTempView("test");

            System.out.println("3.--------start run task");
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            Dataset<Row> ds1 = spark.sql(getTaskString(taskType));

            //if ("task4".equals(taskType)) {
            ds1.write().format("orc").save("s3a://pyunspark/x86_arm_test_1_target1/" + taskType + "/" + arch + "/" + LocalDateTime.now().toString());
            //}
            stopWatch.stop();
            System.out.println("4.--------task end!!,执行时长：" + stopWatch.getTime() + " 秒.");
            //ds1.show();
            //ctas
            //String sql = "create table test stored as orc location 's3a://pyunspark/x86_arm_test_1_target1/' select * from a";


        } catch (Exception e) {
            e.printStackTrace();
        }
        spark.stop();
    }

    public static JavaRDD<Row> schemaRDD(JavaRDD<Row> source) {
        JavaRDD<Row> rowRDD = source.map(new Function<Row, Row>() {
            public Row call(Row row) throws Exception {
                String[] parts = row.toString().split("\001");
                String col1 = parts[0];
                String col2 = parts[1];
                String col3 = parts[2];
                String col4 = parts[3];
                String col5 = parts[4];
                String col6 = parts[5];
                long col7 = parseLong(parts[6]);
                String col8 = parts[7];
                String col9 = parts[8];
                String col10 = parts[9];
                String col11 = parts[10];
                String col12 = parts[11];
                double col13 = parseDouble(parts[12]);
                double col14 = parseDouble(parts[13]);
                String col15 = parts[14];
                long col16 = parseLong(parts[15]);
                long col17 = parseLong(parts[16]);
                long col18 = parseLong(parts[17]);
                long col19 = parseLong(parts[18]);
                long col20 = parseLong(parts[19]);
                String col21 = parts[20];
                String col22 = parts[21];
                long col23 = parseLong(parts[22]);
                long col24 = parseLong(parts[23]);
                long col25 = parseLong(parts[24]);
                long col26 = parseLong(parts[25]);
                long col27 = parseLong(parts[26]);
                long col28 = parseLong(parts[27]);
                long col29 = parseLong(parts[28]);
                long col30 = parseLong(parts[29]);
                long col31 = parseLong(parts[30]);
                long col32 = parseLong(parts[31]);
                long col33 = parseLong(parts[32]);
                long col34 = parseLong(parts[33]);
                long col35 = parseLong(parts[34]);
                long col36 = parseLong(parts[35]);
                long col37 = parseLong(parts[36]);
                long col38 = parseLong(parts[37]);
                long col39 = parseLong(parts[38]);
                long col40 = parseLong(parts[39]);
                long col41 = parseLong(parts[40]);
                long col42 = parseLong(parts[41]);
                long col43 = parseLong(parts[42]);
                long col44 = parseLong(parts[43]);
                long col45 = parseLong(parts[44]);
                long col46 = parseLong(parts[45]);
                long col47 = parseLong(parts[46]);
                long col48 = parseLong(parts[47]);
                long col49 = parseLong(parts[48]);
                long col50 = parseLong(parts[49]);
                long col51 = parseLong(parts[50]);
                long col52 = parseLong(parts[51]);
                long col53 = parseLong(parts[52]);
                long col54 = parseLong(parts[53]);
                double col55 = parseDouble(parts[54]);
                double col56 = parseDouble(parts[55]);
                double col57 = parseDouble(parts[56]);
                double col58 = parseDouble(parts[57]);
                double col59 = parseDouble(parts[58]);
                double col60 = parseDouble(parts[59]);
                double col61 = parseDouble(parts[60]);
                double col62 = parseDouble(parts[61]);
                double col63 = parseDouble(parts[62]);
                double col64 = parseDouble(parts[63]);
                long col65 = parseLong(parts[64]);
                long col66 = parseLong(parts[65]);
                long col67 = parseLong(parts[66]);
                long col68 = parseLong(parts[67]);
                long col69 = parseLong(parts[68]);
                long col70 = parseLong(parts[69]);
                long col71 = parseLong(parts[70]);
                long col72 = parseLong(parts[71]);
                long col73 = parseLong(parts[72]);
                long col74 = parseLong(parts[73]);
                long col75 = parseLong(parts[74]);
                long col76 = parseLong(parts[75]);
                long col77 = parseLong(parts[76]);
                long col78 = parseLong(parts[77]);
                double col79 = parseDouble(parts[78]);
                double col80 = parseDouble(parts[79]);
                double col81 = parseDouble(parts[80]);
                long col82 = parseLong(parts[81]);
                long col83 = parseLong(parts[82]);
                long col84 = parseLong(parts[83]);
                long col85 = parseLong(parts[84]);
                long col86 = parseLong(parts[85]);
                long col87 = parseLong(parts[86]);
                long col88 = parseLong(parts[87]);
                long col89 = parseLong(parts[88]);
                long col90 = parseLong(parts[89]);
                long col91 = parseLong(parts[90]);
                long col92 = parseLong(parts[91]);
                long col93 = parseLong(parts[92]);
                long col94 = parseLong(parts[93]);
                long col95 = parseLong(parts[94]);
                long col96 = parseLong(parts[95]);
                long col97 = parseLong(parts[96]);
                long col98 = parseLong(parts[97]);
                long col99 = parseLong(parts[98]);
                long col100 = parseLong(parts[99]);
                long col101 = parseLong(parts[100]);
                long col102 = parseLong(parts[101]);
                long col103 = parseLong(parts[102]);
                long col104 = parseLong(parts[103]);
                long col105 = parseLong(parts[104]);
                long col106 = parseLong(parts[105]);
                long col107 = parseLong(parts[106]);
                long col108 = parseLong(parts[107]);
                String col109 = parts[108];
                long col110 = parseLong(parts[109]);
                long col111 = parseLong(parts[110]);
                long col112 = parseLong(parts[111]);
                long col113 = parseLong(parts[112]);
                long col114 = parseLong(parts[113]);
                double col115 = parseDouble(parts[114]);
                String col116 = parts[115];
                String col117 = parts[116];
                String col118 = parts[117];
                String col119 = parts[118];
                String col120 = parts[119];
                String col121 = parts[120];
                String col122 = parts[121];
                String col123 = parts[122];
                String col124 = parts[123];
                String col125 = parts[124];
                String col126 = parts[125];
                String col127 = parts[126];
                String col128 = parts[127];
                return RowFactory.create(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16, col17, col18, col19, col20, col21, col22, col23, col24, col25, col26, col27, col28, col29, col30, col31, col32, col33, col34, col35, col36, col37, col38, col39, col40, col41, col42, col43, col44, col45, col46, col47, col48, col49, col50, col51, col52, col53, col54, col55, col56, col57, col58, col59, col60, col61, col62, col63, col64, col65, col66, col67, col68, col69, col70, col71, col72, col73, col74, col75, col76, col77, col78, col79, col80, col81, col82, col83, col84, col85, col86, col87, col88, col89, col90, col91, col92, col93, col94, col95, col96, col97, col98, col99, col100, col101, col102, col103, col104, col105, col106, col107, col108, col109, col110, col111, col112, col113, col114, col115, col116, col117, col118, col119, col120, col121, col122, col123, col124, col125, col126, col127, col128);
            }
        });
        return rowRDD;
    }

    public static StructType generateSchema(JavaRDD<Row> rowRDD) {
        ArrayList<StructField> fields = new ArrayList<StructField>();
        int[] stringId = {1, 2, 3, 4, 5, 6, 8, 9, 10, 11, 12, 15, 21, 22, 109, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128};
        int[] doubleId = {13, 14, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 79, 80, 81, 115};
        for (int i = 1; i <= 128; i++) {
            if (ArrayUtils.contains(stringId, i)) {
                fields.add(DataTypes.createStructField("col" + i, DataTypes.StringType, true));
            } else if (ArrayUtils.contains(doubleId, i)) {
                fields.add(DataTypes.createStructField("col" + i, DataTypes.DoubleType, true));
            } else {
                fields.add(DataTypes.createStructField("col" + i, DataTypes.LongType, true));
            }
        }
        StructType schema = DataTypes.createStructType(fields);
        return schema;
    }

    public static String getTaskString(String taskType) {
        String ret = "";
        if ("task1".equals(taskType)) {
            ret = "select col3,\n" +
                    "    col4\n" +
                    "    col10,\n" +
                    "    col19,\n" +
                    "    col21,\n" +
                    "    col59\n" +
                    "from\n" +
                    "   test\n" +
                    " limit 10000";
        } else if ("task2".equals(taskType)) {
            ret = "select col3,\n" +
                    "    max(col4) as col4,\n" +
                    "    max(col10) as col10,\n" +
                    "    max(col19) as col19,\n" +
                    "    max(col21) as col21,\n" +
                    "    max(col59) as col59\n" +
                    "from\n" +
                    "   test\n" +
                    " group by 1\n" +
                    " limit 10000";
        } else if ("task4".equals(taskType)) {
            ret = "select\n" +
                    "distinct\n" +
                    "  a.col127 as cot01,\n" +
                    "  a.col4 as cot02,\n" +
                    "  a.col7 as cot03,\n" +
                    "  a.col8 as cot04,\n" +
                    "  a.col9 as cot05,\n" +
                    "  a.col10 as cot06\n" +
                    "  ,cot07\n" +
                    "  ,0 as cot08\n" +
                    "  ,cot09\n" +
                    "  ,cot10\n" +
                    "  ,cot11\n" +
                    "  ,cot12\n" +
                    "  ,cot13\n" +
                    "  ,cot14\n" +
                    "  ,cot15\n" +
                    "  ,cot16\n" +
                    "  ,cot17\n" +
                    "  ,cot18\n" +
                    "  ,cot19\n" +
                    "  ,cot20\n" +
                    "  ,cot21\n" +
                    "  ,cot22\n" +
                    "  ,cot23\n" +
                    "  ,cot24\n" +
                    "  ,cot25\n" +
                    "  ,cot26\n" +
                    "  ,cot27\n" +
                    "  ,cot28\n" +
                    "  ,cot29\n" +
                    "  ,cot30\n" +
                    "  ,cot31\n" +
                    "  ,cot32\n" +
                    "  ,cot33\n" +
                    "  ,cot34\n" +
                    "  ,cot35\n" +
                    "  ,cot36\n" +
                    "  ,cot37\n" +
                    "  ,cot38\n" +
                    "  ,cot39\n" +
                    "  ,cot40\n" +
                    "  ,cot41\n" +
                    "  ,cot42\n" +
                    "  ,cot43\n" +
                    "  ,cot44\n" +
                    "  ,cot45\n" +
                    "  ,cot46\n" +
                    "  ,cot47\n" +
                    "  ,cot48\n" +
                    "  ,cot49\n" +
                    "  ,cot50\n" +
                    "  ,cot51\n" +
                    "  ,cot52\n" +
                    "  ,cot53\n" +
                    "  ,cot54\n" +
                    "  ,cot55\n" +
                    "  ,cot56\n" +
                    "  ,cot57\n" +
                    "  ,cot58\n" +
                    "  ,cot59\n" +
                    "  ,cot60\n" +
                    "  ,cot61\n" +
                    "  ,cot62\n" +
                    "  ,cot63\n" +
                    "  ,cot64\n" +
                    "  ,cot65\n" +
                    "  ,cot66\n" +
                    "  ,cot67\n" +
                    "  ,cot68\n" +
                    "  ,col115 as cot69\n" +
                    "  ,cot70\n" +
                    "  ,cot71\n" +
                    "  ,cot72\n" +
                    "  ,cot73\n" +
                    "  ,cot74\n" +
                    "  ,cot75\n" +
                    "  ,cot76\n" +
                    "  ,cot77\n" +
                    "  ,cot78\n" +
                    "  ,cot79\n" +
                    "  ,cot80\n" +
                    "  ,cot81\n" +
                    "  ,cot82\n" +
                    "  ,cot83\n" +
                    "  ,cot84\n" +
                    "  ,cot85\n" +
                    "  ,cot86\n" +
                    "  ,cot87\n" +
                    "  ,cot88\n" +
                    "  ,cot89\n" +
                    "  ,cot90\n" +
                    "  ,cot91\n" +
                    "  ,cot92\n" +
                    "  ,cot93\n" +
                    "  ,cot94\n" +
                    "  ,cot95\n" +
                    "  ,cot96\n" +
                    "  ,cot97\n" +
                    "  ,cot98\n" +
                    "  ,cot99\n" +
                    "  ,cot100\n" +
                    "  ,cot101\n" +
                    "  ,cot102\n" +
                    "  ,cot103\n" +
                    "  ,cot104\n" +
                    "  ,cot105\n" +
                    "  ,cot106\n" +
                    "  ,cot107\n" +
                    "  ,cot108\n" +
                    "  ,cot109\n" +
                    "  ,cot110\n" +
                    "  ,cot111\n" +
                    "  ,cot112\n" +
                    "  ,cot113\n" +
                    "  ,cot114\n" +
                    "  ,cot115\n" +
                    "  ,cot116\n" +
                    "  ,cot117\n" +
                    "  ,col77 as cot118\n" +
                    "  ,col78 as cot119\n" +
                    "  ,cot120\n" +
                    "  ,col79 as cot121\n" +
                    "  ,cot122\n" +
                    "  ,cot123\n" +
                    "  ,col71 as cot124\n" +
                    "  ,col72 as cot125\n" +
                    "  ,cot126\n" +
                    "  ,cot127\n" +
                    "  ,col80 as cot128\n" +
                    "  ,cot129\n" +
                    "  ,col81 as cot130\n" +
                    "  ,cot131\n" +
                    "  ,col27 as cot132\n" +
                    "  ,cot133\n" +
                    "  ,col107 as cot134\n" +
                    "from\n" +
                    "  (select\n" +
                    "    coalesce(col127,'all') as col127\n" +
                    "    ,coalesce(col4,'all') as col4\n" +
                    "    ,coalesce(col128,'all') as col7\n" +
                    "    ,coalesce(col8,'all') as col8\n" +
                    "    ,coalesce(col9,'all') as col9\n" +
                    "    ,coalesce(col10,'all') as col10\n" +
                    "    ,count(distinct col3) as cot07\n" +
                    "    ,count(distinct if(col23>0,col3,null)) as cot09\n" +
                    "    ,count(distinct if(col23=0 and col24>0,col3,null)) as cot10\n" +
                    "    ,count(distinct if(col23=0 and col24=0 and col26+col25>0,col3,null)) as cot11\n" +
                    "    ,count(distinct if(col23=0 and col24=0 and col26+col25=0 and col28>0,col3,null)) as cot12\n" +
                    "    ,count(distinct if(col23=0 and col24=0 and col26+col25=0 and col28=0,col3,null)) as cot13\n" +
                    "    ,count(distinct if(col23>0 and col24=0 and col26+col25=0,col3,null)) as cot14\n" +
                    "    ,count(distinct if(col23>0 and col24>0 and col26+col25=0,col3,null)) as cot16\n" +
                    "    ,count(distinct if(col23>0 and col24=0 and col26+col25>0,col3,null)) as cot17\n" +
                    "    ,count(distinct if(col23>0 and col24>0 and col26+col25>0,col3,null)) as cot19\n" +
                    "    ,count(distinct if(col23=0 and col24>0 and col26+col25=0,col3,null)) as cot15\n" +
                    "    ,count(distinct if(col23=0 and col24>0 and col26+col25>0,col3,null)) as cot18\n" +
                    "    ,count(distinct case when col21 in ('sp','tpcc','tppc','tpuc') then col3 end) as cot20\n" +
                    "    ,count(distinct case when col21 in ('tss','tsr') then col3 end) as cot21\n" +
                    "    ,count(distinct case when col21 in('ta','tc','tds','tmc','tp','tsx','tsr','tsp','tv','tse','tr','tdd','tm3' ) then col3 end) as cot22\n" +
                    "    ,count(distinct case when col21 in ('tj') then col3 end) as cot23\n" +
                    "    ,count(distinct case when col21='tpcc' then col3 end) as cot24\n" +
                    "    ,count(distinct case when col21='tppc' then col3 end) as cot25\n" +
                    "    ,count(distinct case when col21='tpuc' then col3 end) as cot26\n" +
                    "    ,count(distinct case when col21='sp' then col3 end) as cot27\n" +
                    "    ,count(distinct case when col21='tc' then col3 end) as cot28\n" +
                    "    ,count(distinct case when col21='tsr' then col3 end) as cot29\n" +
                    "    ,count(distinct case when col21='ta' then col3 end) as cot30\n" +
                    "    ,count(distinct case when col21='tds' then col3 end) as cot31\n" +
                    "    ,count(distinct case when col21='tmc' then col3 end) as cot32\n" +
                    "    ,count(distinct case when col21='tp' then col3 end) as cot33\n" +
                    "    ,count(distinct case when col21='tsx' then col3 end) as cot34\n" +
                    "    ,count(distinct case when col21='tsp' then col3 end) as cot35\n" +
                    "    ,count(distinct case when col21='tv' then col3 end) as cot36\n" +
                    "    ,count(distinct case when col21='tse' then col3 end) as cot37\n" +
                    "    ,count(distinct case when col21='tr' then col3 end) as cot38\n" +
                    "    ,count(distinct case when col21='tdd' then col3 end) as cot39\n" +
                    "    ,count(distinct case when col21='tm3' then col3 end) as cot40\n" +
                    "    ,count(distinct case when col21='tsr' then col3 end) as cot41\n" +
                    "    ,count(distinct case when col21='tss' then col3 end) as cot42\n" +
                    "    ,count(distinct case when col21 is null or col21='ot' then col3 end) as fnu\n" +
                    "    ,count(distinct case when col17=0 then col3 end) as cot43\n" +
                    "    ,count(distinct case when col17>0 and col17<=7 then col3 end) as cot44\n" +
                    "    ,count(distinct case when col17>7 and col17<=14 then col3 end) as cot45\n" +
                    "    ,count(distinct case when col17>14 and col17<=30 then col3 end) as cot46\n" +
                    "    ,count(distinct case when col17>30 and col17<=60 then col3 end) as cot47\n" +
                    "    ,count(distinct case when col17>60 and col17<=90 then col3 end) as cot48\n" +
                    "    ,count(distinct case when col17>90 and col17<=180 then col3 end) as cot49\n" +
                    "    ,count(distinct case when col17>180 then col3 end) as cot50\n" +
                    "    ,count(distinct case when col18=0 then col3 end) as cot51\n" +
                    "    ,count(distinct case when col18=1 then col3 end) as cot52\n" +
                    "    ,count(distinct case when col18=2 then col3 end) as cot53\n" +
                    "    ,count(distinct case when col18=3 then col3 end) as cot54\n" +
                    "    ,count(distinct case when col18=4 then col3 end) as cot55\n" +
                    "    ,count(distinct case when col18=5 then col3 end) as cot56\n" +
                    "    ,count(distinct case when col18=6 then col3 end) as cot57\n" +
                    "    ,count(distinct case when col18=7 then col3 end) as cot58\n" +
                    "    ,count(distinct case when col18>7 and col18<=14 then col3 end) as cot59\n" +
                    "    ,count(distinct case when col18>14 and col18<=30 then col3 end) as cot60\n" +
                    "    ,count(distinct case when col18>30 and col18<=60 then col3 end) as cot61\n" +
                    "    ,count(distinct case when col18>60 and col18<=90 then col3 end) as cot62\n" +
                    "    ,count(distinct case when col18>90 and col18<=180 then col3 end) as cot63\n" +
                    "    ,count(distinct case when col18>180 then col3 end) as cot64\n" +
                    "    ,sum(col23+ col24+col26+col25) as cot65\n" +
                    "    ,count(distinct case when col23+ col24+col26+col25>0 then col3 end) as cot66\n" +
                    "    ,sum(col20-col19)+count(col19) as cot67\n" +
                    "    ,count(distinct case when col20>0 and col19>0 then col3 end ) as cot68\n" +
                    "    ,sum(col115) as col115\n" +
                    "    ,count(distinct case when col110+coalesce(col42,0)+coalesce(col95,0)+coalesce(col102,0)+coalesce(col103,0)+coalesce(col98,0)+coalesce(col97,0)+coalesce(col99,0)+coalesce(col96,0)+coalesce(col106,0)+coalesce(col101,0)+coalesce(col100,0)+col111+col112+col113+col114>0 then col3 end) as cot70\n" +
                    "    ,count(distinct case when col110+coalesce(col39,0)+coalesce(col95,0)+coalesce(col102,0)+coalesce(col103,0)+coalesce(col98,0)+coalesce(col97,0)+coalesce(col99,0)+coalesce(col96,0)+coalesce(col106,0)+coalesce(col101,0)+coalesce(col100,0)+col111+col112+col113+col114>0 then col3 end) as cot71\n" +
                    "    ,count(distinct case when coalesce(col42,0)+coalesce(col95,0)+coalesce(col102,0)+coalesce(col103,0)+coalesce(col98,0)+coalesce(col97,0)+coalesce(col99,0)+coalesce(col96,0)+coalesce(col106,0)+coalesce(col101,0)+coalesce(col100,0)+col111+col112+col113+col114>0 then col3 end) as cot72\n" +
                    "    ,count(distinct case when coalesce(col39,0)+coalesce(col95,0)+coalesce(col102,0)+coalesce(col103,0)+coalesce(col98,0)+coalesce(col97,0)+coalesce(col99,0)+coalesce(col96,0)+coalesce(col106,0)+coalesce(col101,0)+coalesce(col100,0)+col111+col112+col113+col114>0 then col3 end) as cot73\n" +
                    "    ,count(distinct case when col110>0 then col3 end) as cot74\n" +
                    "    ,count(distinct case when col42>0 then col3 end) as cot75\n" +
                    "    ,count(distinct case when col43>0 then col3 end) as cot76\n" +
                    "    ,count(distinct case when col44>0 then col3 end) as cot77\n" +
                    "    ,count(distinct case when col39>0 then col3 end) as cot78\n" +
                    "    ,sum(col39) as cot84\n" +
                    "    ,count(distinct case when col49+col54>0 then col3 end) as cot79\n" +
                    "    ,count(distinct case when col45+col50>0 then col3 end) as cot80\n" +
                    "    ,count(distinct case when col47+col52>0 then col3 end) as cot81\n" +
                    "    ,count(distinct case when col46+col51>0 then col3 end) as cot82\n" +
                    "    ,count(distinct case when col48+col53>0 then col3 end) as cot83\n" +
                    "    ,count(col49+col54>0) as cot85\n" +
                    "    ,count(col45+col50>0) as cot86\n" +
                    "    ,count(col47+col52>0) as cot87\n" +
                    "    ,count(col46+col51>0) as cot88\n" +
                    "    ,count(col48+col53>0) as cot89\n" +
                    "    ,count(distinct case when coalesce(col95,0)+coalesce(col102,0)+coalesce(col103,0)+coalesce(col98,0)+coalesce(col97,0)+coalesce(col99,0)+coalesce(col96,0)+coalesce(col106,0)+coalesce(col101,0)+coalesce(col100,0)+col111+col112+col113+col114>0 then col3 end) as cot90\n" +
                    "    ,count(distinct case when coalesce(col95,0)>0 then col3 end) as cot91\n" +
                    "    ,count(distinct case when coalesce(col102,0)>0 then col3 end) as cot92\n" +
                    "    ,count(distinct case when coalesce(col103,0)>0 then col3 end) as cot93\n" +
                    "    ,count(distinct case when coalesce(col98,0)+coalesce(col97,0)+coalesce(col99,0)+coalesce(col96,0)+coalesce(col106,0)+coalesce(col101,0)+coalesce(col100,0)+col111+col112+col113+col114>0 then col3 end) as cot94\n" +
                    "    ,count(distinct case when coalesce(col98,0)>0 then col3 end) as cot95\n" +
                    "    ,count(distinct case when coalesce(col97,0)>0 then col3 end) as cot96\n" +
                    "    ,count(distinct case when coalesce(col99,0)>0 then col3 end) as cot97\n" +
                    "    ,count(distinct case when coalesce(col96,0)>0 then col3 end) as cot98\n" +
                    "    ,count(distinct case when coalesce(col106,0)>0 then col3 end) as cot99\n" +
                    "    ,count(distinct case when coalesce(col101,0)>0 then col3 end) as cot100\n" +
                    "    ,count(distinct case when coalesce(col100,0)>0 then col3 end) as cot101\n" +
                    "    ,count(distinct case when col111>0 then col3 end) as cot102\n" +
                    "    ,count(distinct case when col112>0 then col3 end) as cot103\n" +
                    "    ,count(distinct case when col113>0 then col3 end)  as cot104\n" +
                    "    ,count(distinct case when col114>0 then col3 end) as cot105\n" +
                    "    ,count(distinct case when col108>0 then col3 end) as cot106\n" +
                    "    ,count(distinct case when col88>0 then col3 end) as cot107\n" +
                    "    ,count(distinct case when col87>0 then col3 end) as cot108\n" +
                    "    ,count(distinct case when col82>0 then col3 end) as cot109\n" +
                    "    ,count(distinct case when col83>0 then col3 end) as cot110\n" +
                    "    ,count(distinct case when col84>0 then col3 end) as cot111\n" +
                    "    ,count(distinct case when col85>0 then col3 end) as cot112\n" +
                    "    ,count(distinct case when col86>0 then col3 end) as cot113\n" +
                    "    ,count(distinct case when col89>0 then col3 end) as cot114\n" +
                    "    ,count(distinct case when col90>0 then col3 end) as cot115\n" +
                    "    ,count(distinct case when col77>0 then col3 end) as cot116\n" +
                    "    ,count(distinct case when col78>0 then col3 end) as cot117\n" +
                    "    ,sum(col77) as col77\n" +
                    "    ,sum(col78) as col78\n" +
                    "    ,count(distinct case when col77-col78>0 then col3 end) as cot120\n" +
                    "    ,sum(col79) as col79\n" +
                    "    ,count(distinct case when col71>0 then col3 end) as cot122\n" +
                    "    ,count(distinct case when col72>0 then col3 end) as cot123\n" +
                    "    ,sum(col71) as col71\n" +
                    "    ,sum(col72) as col72\n" +
                    "    ,count(distinct case when col71-col72>0 then col3 end) as cot126\n" +
                    "    ,count(distinct case when col80>0 then col3 end) as cot127\n" +
                    "    ,sum(coalesce(col80,0)) col80\n" +
                    "    ,count(distinct case when col81>0 then col3 end) as cot129\n" +
                    "    ,sum(coalesce(col81,0)) col81\n" +
                    "    ,count(distinct case when col27>0 then col3 end) as cot131\n" +
                    "    ,sum(col27) col27\n" +
                    "    ,count(distinct case when col107>0 then col3 end) as cot133\n" +
                    "    ,sum(col107) col107\n" +
                    "    from test\n" +
                    "    group by cube(col127,col4,col128,col8,col9,col10)\n" +
                    "  )a";
        }
        return ret;
    }

    public static long parseLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (Exception e) {
            return 0;
        }
    }

    public static double parseDouble(String s) {
        try {
            return Double.parseDouble(s);
        } catch (Exception e) {
            return 0.0;
        }
    }
}
