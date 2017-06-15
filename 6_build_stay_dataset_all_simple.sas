/************************************************************************************************************
                               SAS Program Description

PROGRAM NAME: 00B_build_stay_dataset2

FOR USE WITH: ALL MEASURES

PURPOSE:      Build Stay level dataset 

OVERVIEW:     

INPUT DATA: 

OUTPUT FILES: 
	data_sty.&stay_dataset_all.

************************************************************************************************************/

OPTIONS COMPRESS=YES REUSE=YES MLOGIC NOMPRINT NOMACROGEN NOSYMBOLGEN STIMER FULLSTIMER OBS=MAX ;

%let include = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Programs/MainAnalysis;
%include "&include./00_Assign_Macro_Variables_and_Libraries.sas";

DATA _NULL_;
StartDate = "%SYSFUNC(DATE(),WORDDATE.)";
StartTime = "%SYSFUNC(TIME(),TIME.)";
PUT "===================================================================================";
PUT "  STARTING JOB INFORMATION:" /;
put "  Job Name: 00_build_stay_dataset" /;
PUT "  Start Date: " StartDate ;
PUT "  Start Time: " StartTime ;
PUT "===================================================================================";
RUN;


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
%remove(ds=data_sty.stay_dataset_all);
/**********************************************************************************************
**********************************************************************************************/

/*Append the final Inpatient and the final PAC stay files to create a file with every desired MedPAR stay*/
DATA data_sty.stay_dataset_all;
	set data_sty.&stay_dataset.
		data_pta.pta_in_base_dataset_pac;
	test_disch=year(disch);
	test_adm=year(admit);
	miss_snf_disch=(disch=. & admit~=. & factype="SNF");
	miss_mds_disch=(disch=. & admit~=. & factype="MDS");

	/*Some SNFs have no discharge. http://ftp.cdc.gov/pub/health_statistics/nchs/datalinkage/cms/cms_analytic_issues_final_2007.pdf (page 6)
	So we impute a discharge date based on the number of utilization days. Also relevant variables, though not as reliable, are cvrlvldt and los, and loscnt*/
	if factype="SNF" & util_day~=. & admit~=. & disch=. then disch=admit+util_day; 
	else if factype="SNF" & util_day=. & admit~=. & disch=. then disch=admit+los; /*No util_day for SNFs pre-2006*/ 

run;

proc freq data=data_sty.stay_dataset_all;
	tables test_disch*factype test_adm*factype test_disch*(miss_snf_disch miss_mds_disch) test_adm*factype / missing;
run;

PROC SORT DATA=data_sty.stay_dataset_all;
     BY hicno admit disch txflag provid;
RUN;

***-----------------------------------***;
***  Add nobs                         ***;
***-----------------------------------***;
DATA data_sty.stay_dataset_all; 
	SET data_sty.stay_dataset_all (DROP=nobs test_disch test_adm);
	nobs = _N_;
RUN;


%macro nobs(data,name); /*Testing the observation counting macro. BR 5-9-17*/
%global &name.; 
	data _null_;
		if 0 then set &data. nobs=count; 
		call symput ("&name.",left(put(count,9.))); 
		stop; 
	run; 

	%put Summary Report - Number of obs in &data.: &obs.;
%mend nobs;
%nobs(data=data_sty.&stay_dataset.,name=obs); 
%nobs(data=data_pta.pta_in_base_dataset_pac,name=obs);
%nobs(data=data_sty.stay_dataset_all,name=obs);
