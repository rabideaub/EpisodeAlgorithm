/************************************************************************************************************
                               SAS Program Description

PROGRAM NAME: 4_bundled_index_cases

FOR USE WITH: ALL MEASURES

PURPOSE:      create a bundled index file for use in defining the history and post index files 

OVERVIEW: 	  create an index file that includes one record per bene with arrayed fields for 
			  each admit, discharge and provider to use in finding records for the history 
			  and post files

INPUT DATA: 
	dx_data.index03  *PRE-SORTED BY HICNO

OUTPUT FILES: 
	dx_data.indexMatch
	dx_data.index_max_case

************************************************************************************************************/

OPTIONS COMPRESS=YES;

DATA _NULL_;
	StartDate = "%SYSFUNC(DATE(),WORDDATE.)";
	StartTime = "%SYSFUNC(TIME(),TIME.)";
	PUT "===================================================================================";
	PUT "  STARTING JOB INFORMATION:" /;
	PUT "  Job Name: 4_bundled_index_cases" /;
	PUT "  Start Date: " StartDate ;
	PUT "  Start Time: " StartTime ;
	PUT "===================================================================================";
RUN;

%let include = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Programs/MainAnalysis;
%include "&include./00_Assign_Macro_Variables_and_Libraries.sas";


/*********************************************************************************************
*********************************************************************************************/
/*If this program has been run multiple times, delete its output dataset (if it exists)
  so that if an problem occurs we don't accidentally use a previous iteration of the dataset
  and are forced to correct the problem*/
%macro remove(ds);
	%if %sysfunc(exist(&ds.)) = 1 %then %do;
		proc delete data=&ds.;
	%end;
%mend;
%remove(ds=dx_data.indexMatch);
/**********************************************************************************************
**********************************************************************************************/


**********************************************************************
* DETERMINE THE MAXIMUM NUMBER OF CASES TO USE TO BUNDLE CLAIMS.
**********************************************************************;
PROC MEANS DATA=dx_data.index03 MAX;                                                
	VAR case;                                                                
	OUTPUT OUT=dx_data.index_max_case MAX=index_max_case;                                             
RUN;  
 
DATA _NULL_;
	SET dx_data.index_max_case;
	CALL SYMPUT("MC", LEFT(PUT(index_max_case, 3.)));
RUN;
%PUT MAX CASE FOR INDEX: &MC.;
**********************************************************************;



**********************************************************************
* BUNDLE CLAIMS FOR CREATING PRE AND POST FILES
**********************************************************************;
 DATA dx_data.indexMatch (KEEP=hicno admdt1-admdt&MC disdt1-disdt&MC provid1-provid&MC index_max_case
						  SORTEDBY=hicno);
	SET dx_data.index03;
	BY hicno;

	INFORMAT provid1-provid&MC $6.;
	FORMAT admdt1-admdt&MC disdt1-disdt&MC MMDDYY10.;
	RETAIN admdt1-admdt&MC disdt1-disdt&MC provid1-provid&MC;
	ARRAY ADM(&MC) admdt1-admdt&MC;
	ARRAY DIS(&MC) disdt1-disdt&MC;
	ARRAY PVD(&MC) $provid1-provid&MC;

	* INITIALIZE ADMISSIONS, DISCHARGES, PROVID & CASE/TRANS TO MISSING;
	IF (FIRST.hicno) THEN DO;
		DO I=1 TO &MC;
			ADM(I)=.;
			DIS(I)=.;
			PVD(I)='';
		END;
	END;

	index_max_case = &MC.;

	* CREATE A STRING OF INDEX FIELDS FOR EACH BENE;
	ADM(CASE)=admit;
	DIS(CASE)=disch;
	PVD(CASE)=provid;

	* OUTPUT IF LAST INDEX ADMISSION FOR THIS BENE;
	IF (LAST.hicno) THEN OUTPUT;
RUN;


TITLE  "indexMatch: PERSON LEVEL FILE";
TITLE2 "WITH ONE RECORD PER HICNO LISTING ALL CASES";
PROC CONTENTS DATA=dx_data.indexMatch;
RUN;

/*PRINT OFF TEN RECORDS THAT HAVE AT LEAST 5 CASES*/
PROC PRINT DATA=dx_data.indexMatch(OBS=10);
	FORMAT admdt1-admdt&MC disdt1-disdt&MC MMDDYY8.;
	WHERE provid5 NE '';
RUN;
**********************************************************************;



TITLE;
TITLE2;


%macro nobs(data,name); /*Testing the observation counting macro. BR 5-9-17*/
%global &name.; 
	data _null_;
		if 0 then set &data. nobs=count; 
		call symput ("&name.",left(put(count,9.))); 
		stop; 
	run; 

	%put Summary Report - Number of obs in &data.: &obs.;
%mend nobs;
%nobs(data=dx_data.index03,name=obs); 
%nobs(data=dx_data.indexMatch,name=obs); 


DATA _NULL_;
EndDate = "%SYSFUNC(DATE(),WORDDATE.)";
EndTime = "%SYSFUNC(TIME(),TIME.)";
PUT "===================================================================================";
PUT "  ENDING JOB INFORMATION:" /;
PUT "  Job Name: 4_bundled_index_cases" /;
PUT "  End Date: " EndDate ;
PUT "  End Time: " EndTime ;
PUT "===================================================================================";
RUN;
