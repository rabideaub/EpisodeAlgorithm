/************************************************************************************************************
************************************************************************************************************
Program: gen_cov_dataset
Created By: Brendan Rabideau
Created Date: 5/15/15
Updated Date: 6/14/16
Purpose: The purpose of this program is to to generate the raw datasets used in Yale's Analytic Files, 
		 which create datasets to be used in Yale's 30day Unplanned Readmission Algorithim. The program creates
		 the coverage dataset.

Notes: This program actually skips a step. We did not have exact matches for Yale's Analytic Files, but we 
	   do have data that looks like the output of one of their Analytic File programs (0B_format_coverage_and_hospice.sas).
	   This program attempts to create the output of that program. Note that since we do not have the raw data, 
	   more assumptions go into the creation of this dataset than into others.

	   We have no access to hospice info, unlike Yale's coverage dataset.

Updates:
	6/12/15 - Added years going back to 2002
	7/06/15 - Updated to drop dup bene_id's
	9/17/15 - Updated to include 2012
	10/30/15 - Updated to include 2013 - the directory structure is non-parallel, so this is hardcoded. May need updating later
	11/24/15- Converting for the HRRP project
	11/24/14- Making the process of reading in raw data more automated - macro-ing libnames, making start and end year set statements self-generating
	06/14/16- Added in years going back to 2003, including a BID-->bene_id xwalk for 2003-2005
************************************************************************************************************
************************************************************************************************************/

%let out = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Processed;
libname out "&out.";

%let include = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Programs/MainAnalysis;
%include "&include./00_Assign_Macro_Variables_and_Libraries.sas";
%let syr=20&YY.;
%let eyr=20&YYE.;


/*********************************************************************************************
*********************************************************************************************/
/*If this program has been run multiple times, delete its output dataset (if it exists)
  so that if an issue occurs we don't accidentally use a previous iteration of the dataset
  and are forced to correct the issue*/
%macro remove(ds);
	%if %sysfunc(exist(&ds.)) = 1 %then %do;
		proc delete data=&ds.;
	%end;
%mend;
%remove(ds=out.coverage&YYE._&MM.);
/**********************************************************************************************
**********************************************************************************************/

%macro hmo_buy(styear,endyear);
	%do i=&styear. %to &endyear.;
	%let j=%substr(&i,3,2);

		/*Different years require different renaming. 2002-2005*/
		%if 2002<=&i. & &i.<=2005 %then %do;
			libname xw "/schaeffer-b/sch-protected/VERTICAL-INTEGRATION/Source-Copy/Rand_data_20160216/FROM_PAC/c/data/b2006_2008/Request863";
			libname den "/schaeffer-b/sch-protected/VERTICAL-INTEGRATION/Source-Copy/Rand_data_20160216/FROM_PAC/c/data/cms7707/DATA";

			proc contents data=den.denom&i.; run;

			data cov_&i. (keep=BID Y&j.BUY1-Y&j.BUY12 Y&j.HMO1-Y&j.HMO12);
				set den.denom&i.;
				if BID_ ~="";
				rename buyin01-buyin12   = Y&j.BUY1-Y&j.BUY12
					   hmoind01-hmoind12 = Y&j.HMO1-Y&j.HMO12
					   BID_=BID;
			run;

			/*Merge on the BID --> Bene_ID crosswalk*/
			data xwalk;
				set xw.bid_bene_xwalk (where=(BID~=""));
			run;

			proc sort data=cov_&i.; by BID; run;
			proc sort data=xwalk nodupkey; by BID; run;

			data cov_&i. (drop=BID rename=(bene_id=hicno));
				merge cov_&i. (in=a)
					  xwalk (in=b);
				by BID;
				if a & b;
			run;

			proc sort data=cov_&i. nodupkey; by HICNO; run;
		%end;

		/*2006-2008*/
		%if 2006<=&i. & &i.<=2008 %then %do;
			libname den&i. "&den.";

			data cov_&i. (keep=HICNO Y&j.BUY1-Y&j.BUY12 Y&j.HMO1-Y&j.HMO12);
				set den&i..den&i.;
				if bene_id ~="";
				rename BUYIN01-BUYIN12   = Y&j.BUY1-Y&j.BUY12
					   HMOIND01-HMOIND12 = Y&j.HMO1-Y&j.HMO12
					   bene_id = HICNO;
			run;

			proc sort data=cov_&i nodupkey; by HICNO; run;
		%end;

		/*2009-2011*/
		%if &i. >=2009 & &i.<=2011 %then %do;
			libname bsf&i. "&bsf.";

			data cov_&i. (keep=HICNO Y&j.BUY1-Y&j.BUY12 Y&j.HMO1-Y&j.HMO12);
				set bsf&i..mbsf_ab_summary&i.;
				if bene_id ~="";
				rename BUYIN01-BUYIN12   = Y&j.BUY1-Y&j.BUY12
					   HMOIND01-HMOIND12 = Y&j.HMO1-Y&j.HMO12
					   bene_id = HICNO;
			run;

			proc sort data=cov_&i nodupkey; by HICNO; run;
		%end;

		/*2012*/
		%if &i.>=2012 %then %do;
			libname bsf&i. "&bsf.";

			data cov_&i. (keep=HICNO Y&j.BUY1-Y&j.BUY12 Y&j.HMO1-Y&j.HMO12);
				set bsf&i..mbsf_ab_summary&i.;
				if bene_id ~="";
				rename BUYIN01-BUYIN12   = Y&j.BUY1-Y&j.BUY12
					   HMOIND01-HMOIND12 = Y&j.HMO1-Y&j.HMO12
					   bene_id = HICNO;
			run;

			proc sort data=cov_&i nodupkey; by HICNO; run;
		%end;

		/*2013*/ /*Vestigial - This was when the 2013 data was in a different location*/
		/*%if &i.=2013 %then %do;
			libname bsf&i. "&bsf.";

			data cov_&i. (keep=HICNO Y&j.BUY1-Y&j.BUY12 Y&j.HMO1-Y&j.HMO12);
				set bsf&i..mbsf_ab_summary&i.;
				if bene_id ~="";
				rename BUYIN01-BUYIN12   = Y&j.BUY1-Y&j.BUY12
					   HMOIND01-HMOIND12 = Y&j.HMO1-Y&j.HMO12
					   bene_id = HICNO;
			run;

			proc sort data=cov_&i nodupkey; by HICNO; run;
		%end;*/
	%end;
%mend;
%hmo_buy(&syr.,&eyr.);

data out.coverage&YYE._&MM.; /*When we run the whole thing call it coverage&YYE._&MM.*/
	merge cov_:;
	by HICNO;
run;

proc contents data=out.coverage&YYE._&MM. out=cov_dataset_cont (keep=NAME TYPE LENGTH VARNUM LABEL FORMAT);
run;


%macro nobs(data,name); /*Testing the observation counting macro. BR 5-9-17*/
%global &name.; 
	data _null_;
		if 0 then set &data. nobs=count; 
		call symput ("&name.",left(put(count,8.))); 
		stop; 
	run; 

	%put Summary Report - Number of obs in &data.: &obs.;
%mend nobs;
%macro loop;
	%do i=&syr. %to &eyr.;
		%nobs(data=cov_&i.,name=obs); 
	%end;
%mend;
%loop;
%nobs(data=out.coverage&YYE._&MM.,name=obs); 

/*********************
PRINT CHECK
*********************/
/*Output a sample of each of the datasets to an excel workbook*/
ods tagsets.excelxp file="&out./look_coverage.xml" style=sansPrinter;
ods tagsets.excelxp options(absolute_column_width='20' sheet_name="Coverage" frozen_headers='yes');
proc print data=out.coverage&YYE._&MM. (obs=1000);
run;
ods tagsets.excelxp options(absolute_column_width='20' sheet_name="Coverage Contents" frozen_headers='yes');
proc print data=cov_dataset_cont (obs=1000);
run;
ods tagsets.excelxp close;
/*********************
CHECK END
*********************/

