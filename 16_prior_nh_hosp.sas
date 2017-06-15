libname mds "/schaeffer-b/sch-protected/from-projects/50367_Nuckols/rabideau/Data/HRRP/Processed";
libname raw "/schaeffer-b/sch-protected/VERTICAL-INTEGRATION/Data/2009-13_PAC/Raw/MDS";
libname med "/schaeffer-b/sch-protected/VERTICAL-INTEGRATION/Data/2009-13_PAC/Raw/MedPAR";
libname final "/schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Final";
libname out "/schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Final";
%let out = /schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Output;
%let include=/schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Programs/MainAnalysis;

%include "&include./00_Assign_Macro_Variables_and_Libraries.sas";

/*Summarize episodes per quarter for each of the 2 ways to construct the MDS.*/
data mds1;
	set mds.mds_stays_2006_2014_1; /*Simple algorithm, combine datasets first then determine stays*/
	if disch_dt>"01jan06"d;
	version=1;
run;

data mds2;
	set mds.mds_stays_2006_2013_2; /*Sophisticated algorithm, run 2.0 and 3.0 separately then append and smooth stays*/
	if disch_dt>"01jan06"d;
	version=2;
run;

data mds_all;
	set mds1
		mds2;
	qtr=intck('qtr',"01jan06"d,disch_dt)+1;
	year=year(disch_dt);
run;

proc freq data=mds_all;
	tables year*qtr / missing;
run;

ods output CrossTabFreqs=xtab1;
proc freq data=mds_all;
	tables version*qtr  / missing;
run;
ods output close;

/*Test prior nursing home discharges withn 3 days using datasets from each of the 2 ways to construct the MDS*/

	data test;
		set final.freq_array_&dx._snf;
	run;

	%macro loop;
		%do j=1 %to 2;

	/*************************************************************************
	 Flag prior nursing home discharges from raw MDS within 3 days. 
	 Test 2 versions of the MDS.
	*************************************************************************/
			/*Read in the long-file MDS*/
			data mds_disch;
				set mds&j. (keep=bene_id disch_dt);
			run;

			proc print data=mds_disch (obs=10); run;

			proc sort data=mds_disch; by bene_id disch_dt; run;

			/*Make it wide so we can do a many-to-one merge on bene_id later*/
			proc transpose data=mds_disch out=mds_disch_flat prefix=mds_disch_dt;
				by bene_id;
				var disch_dt;
			run;

			proc sort data=test; by hicno; run;

			/*Merge to the index admissions in our SAF*/
			data test (drop=mds_disch_dt:);
				merge test (in=a)
					  mds_disch_flat (in=b rename=(bene_id=hicno));
				by hicno;
				if a;

				prior_nh_v&j.=0;

				array dates {*} mds_disch_dt:;

				/*Loop through all of the wide-form dates and compare to index admission*/
				do i=1 to dim(dates);
					if 0<=(admit-dates[i])<=3 then prior_nh_v&j.=1;
				end;
			run;
		%end;
	%mend;
	%loop;

	title "Check Prior NH Discharges";
	/*Some ODS output for graphs*/
	ods output CrossTabFreqs=xtab2;
	proc freq data=test;
		tables prior_nh_v1*qtr/ missing;
	run;
	ods output close;

	ods output CrossTabFreqs=xtab3;
	proc freq data=test;
		tables prior_nh_v2*qtr/ missing;
	run;
	ods output close;
	title;

	/*************************************************************************
	 Test prior nursing home assessment from raw MDS within 90 or 180 days
	*************************************************************************/

	/*Same method as above. Flatten, many-to-one merge, loop through date array*/
	data mds;
		set raw.mds2006 (keep=bene_id target_date rename=(target_date=target_dt))
			raw.mds2007 (keep=bene_id target_date rename=(target_date=target_dt))
			raw.mds2008 (keep=bene_id target_date rename=(target_date=target_dt))
			raw.mds2009 (keep=bene_id target_date rename=(target_date=target_dt))
			raw.mds2010 (keep=bene_id target_date rename=(target_date=target_dt))
			raw.mds_asmt_summary_3_2010 (keep=bene_id trgt_dt rename=(trgt_dt=target_dt))
			raw.mds_asmt_summary_3_2011 (keep=bene_id trgt_dt rename=(trgt_dt=target_dt))
			raw.mds_asmt_summary_3_2012 (keep=bene_id trgt_dt rename=(trgt_dt=target_dt))
			raw.mds_asmt_summary_3_2013 (keep=bene_id trgt_dt rename=(trgt_dt=target_dt));
	run;

	proc sort data=mds; by bene_id target_dt; run;

	proc transpose data=mds out=mds_flat prefix=mds_target_dt;
		by bene_id;
		var target_dt;
	run;

	proc contents data=mds_flat; run;

	proc print data=mds (obs=100); run;

	proc print data=mds_flat (obs=10);
		var bene_id mds_target_dt1-mds_target_dt10;
	run;

	proc sort data=test; by hicno; run;

	data test (drop=mds_target_dt:);
		merge test (in=a)
			  mds_flat (in=b rename=(bene_id=hicno));
		by hicno;
		if a;

		prior_nh_90=0;
		prior_nh_180=0;

		array dates {*} mds_target_dt:;

		do i=1 to dim(dates);
			if 0<=(admit-dates[i])<=90 then prior_nh_90=1;
			if 0<=(admit-dates[i])<=180 then prior_nh_180=1;
		end;
	run;

	title "Check Prior NH Assessments";
	proc print data=test (obs=20);
		var hicno admit disch prior_nh:;
	run;

	/*Some ODS output for graphs*/
	ods output CrossTabFreqs=xtab4;
	proc freq data=test;
		tables prior_nh_90*qtr / missing;
	run;
	ods output close;

	ods output CrossTabFreqs=xtab5;
	proc freq data=test;
		tables prior_nh_180*qtr / missing;
	run;
	ods output close;
	title;

	/**************************************************************************
	 Sum up all prior hospitalizations (STACH or CAH) within the last year 
	**************************************************************************/

	/*Same method as above. Flatten, many-to-one merge, loop through date array*/
	data med (keep=bene_id dschrgdt);
		set med.medpar2006 (keep=bene_id prvdrnum spclunit dschrgdt)
			med.medpar2007 (keep=bene_id prvdrnum spclunit dschrgdt)
			med.medpar2008 (keep=bene_id prvdrnum spclunit dschrgdt)
			med.medpar2009 (keep=bene_id prvdrnum spclunit dschrgdt)
			med.medpar2010 (keep=bene_id prvdrnum spclunit dschrgdt)
			med.medpar2011 (keep=bene_id prvdrnum spclunit dschrgdt)
			med.medpar2012 (keep=bene_id prvdrnum spclunit dschrgdt)
			med.medpar2013 (keep=bene_id prvdrnum spclunit dschrgdt);

		if (1<=(input(substr(PRVDRNUM,length(PRVDRNUM)-3,4),?? 4.))<=899 & spclunit ~in("R","T","M","S")) | /*STACH or CAH. No IRF or swing beds*/
		(1300<=(input(substr(PRVDRNUM,length(PRVDRNUM)-3,4),?? 4.))<=1399 & spclunit ~in("R","T","M","S"));
	run;

	proc sort data=med; by bene_id dschrgdt; run;

	proc transpose data=med out=med_flat prefix=med_disch;
		by bene_id;
		var dschrgdt;
	run;

	proc contents data=med_flat; run;

	proc print data=med (obs=100); run;

	proc print data=med_flat (obs=10);
		var bene_id med_disch1-med_disch10;
	run;

	proc sort data=test; by hicno; run;

	data out.freq_array_&dx._snf (drop=med_disch:);
		merge test (in=a)
			  med_flat (in=b rename=(bene_id=hicno));
		by hicno;
		if a;

		prior_hosp365=0;

		array dates {*} med_disch:;

		do i=1 to dim(dates);
			if 0<=(admit-dates[i])<=365 then prior_hosp365+1;
		end;
	run;

	title "Check Prior Hospitalizations";
	proc print data=out.freq_array_&dx._snf (obs=20);
		var hicno admit disch prior_hosp365;
	run;

	ods output CrossTabFreqs=xtab6;
	proc freq data=out.freq_array_&dx._snf;
		tables prior_hosp365*qtr / missing;
	run;
	ods output close;
	title;


	/**************************************************************************
	 Make the ODS output pretty
	**************************************************************************/
	%macro loop2(num,var);
		data xtab&num.;
			set xtab&num.;
			if frequency<11 then frequency=.;
			if _TYPE_=11;
		run;
		proc transpose data=xtab&num. out=qtr&num. prefix=qtr;
			by &var.;
			id qtr;
			var frequency;
		run;
		proc print data=qtr&num.; run;
	%mend;
	%loop2(num=1,var=version);
	%loop2(num=2,var=prior_nh_v1);
	%loop2(num=3,var=prior_nh_v2);
	%loop2(num=4,var=prior_nh_90);
	%loop2(num=5,var=prior_nh_180);
	%loop2(num=6,var=prior_hosp365);


/************************************************************************************
PRINT CHECK
************************************************************************************/
/*Output a sample of each of the datasets to an excel workbook*/
ods tagsets.excelxp file="&out./mainanalysis_test_mds.xml" style=sansPrinter;
ods tagsets.excelxp options(absolute_column_width='20' sheet_name="MDS Episode Counts" frozen_headers='yes');
proc print data=qtr1;run;

ods tagsets.excelxp options(absolute_column_width='20' sheet_name="Prior NH Discharge V1" frozen_headers='yes');
proc print data=qtr2;run;

ods tagsets.excelxp options(absolute_column_width='20' sheet_name="Prior NH Discharge V2" frozen_headers='yes');
proc print data=qtr3;run;

ods tagsets.excelxp options(absolute_column_width='20' sheet_name="Prior NH Assessment 90 Days" frozen_headers='yes');
proc print data=qtr4;run;

ods tagsets.excelxp options(absolute_column_width='20' sheet_name="Prior NH Assessment 180 Days" frozen_headers='yes');
proc print data=qtr5;run;

ods tagsets.excelxp options(absolute_column_width='20' sheet_name="Prior Hospitalizations 365 Days" frozen_headers='yes');
proc print data=qtr6;run;


ods tagsets.excelxp close;
/*********************
CHECK END
*********************/

