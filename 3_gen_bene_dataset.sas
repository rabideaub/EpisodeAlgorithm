/************************************************************************************************************
************************************************************************************************************
Program: gen_bene_dataset
Created By: Brendan Rabideau
Created Date: 5/15/15
Updated Date: 6/14/16
Purpose: The purpose of this program is to to generate the raw datasets used in Yale's Analytic Files, 
  		 which create datasets to be used in Yale's 30day Unplanned Readmission Algorithim. This program uses 
  		 beneficiary datasets for select years to create the bene_dataset which is used to create 
  		 the initial index dataset (index01) along with the coverage_dataset and stay_dataset.

Notes: Beneficiary Summary Files have taken 3 different forms from 2002-2011 - denominator with EHIC ID,
       denominator with Bene_ID, and BSF with Bene_ID. and EHIC-Bene xwalk is used to create the data 
       from 2002-2005. The xwalking is done in a different program.
Updates:
	6/12/15 - Added years going back to 2002
	9/17/15 - Updated to include 2012
	11/24/15- Converting for the HRRP project
	11/24/15- Making the process of reading in raw data more automated - macro-ing libnames, making start and end year set statements self-generating
	06/14/16- Added in years going back to 2003, including a BID-->bene_id xwalk for 2003-2005
************************************************************************************************************
************************************************************************************************************/


/*The purpose of this program is to to generate the raw datasets used in Yale's Analytic Files, 
  which create datasets to be used in Yale's Readmission Algorithim.  */

options compress=yes;

%let out = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Raw;
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
%remove(ds=out.bene_dataset);
/**********************************************************************************************
**********************************************************************************************/

/*We have denominator files from 2002-2008, then BSF files from 2009 onwards. Den2002-2005 has a different ID (EHIC)
  so we have special files that are x-walked to bene_id for those years. */

%macro denom(styear,endyear);
%put &styear. &endyear.;
	%do i=&styear. %to &endyear.;
	
		/*2002-2005*/
		%if 2002<=&i. & &i.<=2005 %then %do;
			libname xw "/schaeffer-b/sch-protected/VERTICAL-INTEGRATION/Source-Copy/Rand_data_20160216/FROM_PAC/c/data/b2006_2008/Request863";
			libname den&i. "/schaeffer-b/sch-protected/VERTICAL-INTEGRATION/Source-Copy/Rand_data_20160216/FROM_PAC/c/data/cms7707/DATA";

			proc contents data=den&i..denom&i.; run;
			proc freq data=den&i..denom&i.; tables BENDTHV; run; /*Is this var the EDBVDETH indicator?*/

			data den&i.;
				set den&i..denom&i.;
				rename
					/*STATE    = BSTATE
					COUNTY   = COUNTY*/
					BENDTHV  = EDBVDETH
					RACE     = RACE
					SEX      = SEX
					/*zipcode  = ZIP*/
					BID_	 = BID;

				BIRTH=input(BENE_DOB,ANYDTDTE8.);
				DEATH=input(endthdt,ANYDTDTE8.);
				format BIRTH DEATH DATE9.;

				year=&i.;
			run;

			/*Merge on the BID --> Bene_ID crosswalk*/
			data xwalk;
				set xw.bid_bene_xwalk (where=(BID~=""));
			run;

			proc sort data=den&i.; by BID; run;
			proc sort data=xwalk nodupkey; by BID; run;

			data den&i. (drop=BID rename=(bene_id=hicno));
				merge den&i. (in=a)
					  xwalk (in=b);
				by BID;
				if a & b;
			run;

			proc sort data=den&i. nodupkey; by HICNO; run;
		%end;

		/*2006-2008*/
		%if 2006<=&i. & &i.<=2008 %then %do;
			libname den&i. "&den.";

			proc contents data= den&i..den&i.; run;

			data den&i. (drop=A_MO_CNT B_MO_CNT HMO_MO BUYIN_MO);
				set den&i..den&i.;
				rename
					BENE_DOB = BIRTH
					STATE_CD = BSTATE
					CNTY_CD  = COUNTY
					DEATH_DT = DEATH
					V_DOD_SW = EDBVDETH
					BENE_ID  = HICNO
					RACE     = RACE
					SEX      = SEX
					BENE_ZIP = ZIP;

				year=&i.;
			run;
		%end;

		/*2009+*/
		%if 2009<=&i. %then %do;
			libname den&i. "&bsf.";
			data den&i. (drop=A_MO_CNT B_MO_CNT HMO_MO BUYIN_MO);
				set den&i..mbsf_ab_summary&i.;
				rename
					BENE_DOB = BIRTH
					STATE_CD = BSTATE
					CNTY_CD  = COUNTY
					DEATH_DT = DEATH
					V_DOD_SW = EDBVDETH
					BENE_ID  = HICNO
					RACE     = RACE
					SEX      = SEX
					BENE_ZIP = ZIP;

				year=&i.;
			run;
		%end;
	%end;
%mend;
%denom(&syr.,&eyr.);

/*Only have 1 record per bene_id - make this the most recent record, particularly the most updated death_dt*/
data bene;
	length ZIP $9;
	set den&syr.-den&eyr.;
run;

title "Check Missings Demographic Vars by Year";
proc freq data=bene;
	tables year*(sex race rti_race_cd) / missing;
run;
title;

proc sort data=bene; by hicno year death edbvdeth ; run;

data out.bene_dataset (keep=BIRTH BSTATE COUNTY DEATH EDBVDETH HICNO RACE SEX ZIP RTI_RACE_CD);
	set bene;
	by hicno;
	if last.hicno then output;
run;

proc contents data=out.bene_dataset out=bene_dataset_cont (keep=NAME TYPE LENGTH VARNUM LABEL FORMAT);
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
		%nobs(data=den&i.,name=obs); 
	%end;
%mend;
%loop;
%nobs(data=out.bene_dataset,name=obs); 

/*********************
PRINT CHECK
*********************/
/*Output a sample of each of the datasets to an excel workbook*/
ods tagsets.excelxp file="&out./look_bene.xml" style=sansPrinter;
ods tagsets.excelxp options(absolute_column_width='20' sheet_name="Bene" frozen_headers='yes');
proc print data=out.bene_dataset (obs=1000);
run;
ods tagsets.excelxp options(absolute_column_width='20' sheet_name="Bene Contents" frozen_headers='yes');
proc print data=bene_dataset_cont (obs=1000);
run;
ods tagsets.excelxp close;
/*********************
CHECK END
*********************/

