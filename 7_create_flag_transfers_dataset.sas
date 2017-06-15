/************************************************************************************************************
                               SAS Program Description

PROGRAM NAME: 0A_create_flag_transfers_dataset

PURPOSE:      Prep data for transfer check.  To be used for all measures using the same source data. 

OVERVIEW: 	  identify all transfer stays in stay dataset.  

INPUT DATA: 
	data_sty.&stay_dataset.  *PRE-SORTED BY HICNO ADMIT DISCH TXFLAG PROVID  -WITH nobs created after sort

OUTPUT FILES: 
	data_an.flag_transfers

************************************************************************************************************/

OPTIONS COMPRESS=YES;

DATA _NULL_;
StartDate = "%SYSFUNC(DATE(),WORDDATE.)";
StartTime = "%SYSFUNC(TIME(),TIME.)";
PUT "===================================================================================";
PUT "  STARTING JOB INFORMATION:" /;
PUT "  Job Name: 0A_create_flag_transfers_dataset" /;
PUT "  Start Date: " StartDate ;
PUT "  Start Time: " StartTime ;
PUT "===================================================================================";
RUN;

%let include = /sch-projects/dua-data-projects/VERTICAL-INTEGRATION/rabideau/Programs/MainAnalysis;
%include "&include./00_Assign_Macro_Variables_and_Libraries.sas";

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
%remove(ds=data_an.flag_transfers);
/**********************************************************************************************
**********************************************************************************************/

**********************************************************************
* VALIDATE TXFLAG IN STAY DATASET
**********************************************************************;
TITLE 'VALIDATE TXFLAG - SHOULD BE -1 FOR 2 OR 66, 1 FOR 20 AND 0 OTHERWISE';
PROC FREQ DATA=data_sty.&stay_dataset.;
	TABLE case_type*txflag*ddest /LIST MISSING;
RUN;
**********************************************************************;


**********************************************************************
* FLAG CLAIMS THAT MAKE UP A TRANSFER CHAIN.
**********************************************************************;
DATA flag_transfers
	(KEEP=hicno trans_caseno newobs counts newdiag1 newdrgcd newdisch newadmit newprovid newtxflag
	 RENAME=(newobs=nobs newdiag1 = diag1 newdrgcd = drgcd newdisch = disch newadmit = admit 
			 newprovid = provid newtxflag = txflag));

	/*CREATE LAGGED VARIABLES*/
	LENGTH mark $1 hicno2 $15 prvid2 $6 diag1_2 $7 drgcd_2 $3;
	RETAIN mark '';

	/*ASSIGN FIELDS FROM PRIOR RECORD TO 2 VARIABLES*/
	prvid2=provid;
	hicno2=hicno;
	admdate2=admit;
	disdate2=disch;
	nobs2=nobs;
	txflag2=txflag;
	diag1_2 = diag1;
	drgcd_2=drgcd;

	SET data_sty.&stay_dataset.(KEEP=hicno admit disch nobs txflag provid diag1 drgcd case_type
								WHERE=(case_type='CMS'));

	/*SELECT TRANSFER CLAIMS*/
	/*DIFFERENT PROVIDER, SAME OR NEXT DAY ADMIT AFTER DISCHARGE AND TXFLAG OF 0 OR -1*/
	IF hicno2 = hicno AND prvid2 NE provid AND (disdate2 <= admit <= disdate2+1)
	  AND txflag2<1 THEN DO;
		IF mark='' THEN DO;  /*IF FIRST IDENTIFIED THEN OUTPUT CURRENT AND PREVIOUS STAYS*/
			/*OUTPUT PREVIOUS STAY*/
			trans_caseno+1;
			counts+1;
			mark='1';
			newobs=nobs2;
			newdiag1 = diag1_2;
			newdrgcd= drgcd_2;
			newdisch = disdate2;
			newadmit = admdate2;
			newprovid = prvid2;
			newtxflag = txflag2;
			OUTPUT;
			/*OUTPUT CURRENT STAY*/
			counts+1;
			newobs=nobs;
			newdiag1 = diag1;
			newdrgcd = drgcd;
			newdisch = disch;
			newadmit = admit;
			newprovid = provid;
			newtxflag = txflag;
			OUTPUT;
		END;
		ELSE IF mark='1' THEN DO;  /*IF PREVIOUS STAY WAS OUTPUT THEN ONLY OUTPUT CURRENT STAY*/
			counts+1;
			newobs=nobs;
			newdiag1 = diag1;
			newdrgcd = drgcd;
			newdisch = disch;
			newadmit = admit;
			newprovid = provid;
			newtxflag = txflag;
			OUTPUT;
		END;
	END;
	ELSE DO;  /*RESET COUNTER AND MARK IF NOT A TRANSFER STAY*/
		counts=0;
		mark='';
	END;
RUN;

**********************************************************************
* ADD IN PROC CODES FOR H/K AND CB
**********************************************************************;
PROC SORT DATA=flag_transfers;
	BY nobs;
RUN;

DATA data_an.flag_transfers;
	MERGE flag_transfers(IN=a)
		  data_sty.&stay_dataset.(KEEP=nobs proc1-proc25);
	BY nobs;
	IF a;
RUN;

PROC SORT DATA=data_an.flag_transfers;
	BY hicno trans_caseno;
RUN;

TITLE 'TRANSFER CHAIN COUNTS';
PROC FREQ DATA=data_an.flag_transfers;
	TABLE counts 
		  disch*counts
		/LIST MISSING;
	FORMAT disch MONYY.;
RUN;

TITLE 'PRINT TRANSFER CHAIN SAMPLE TO VALIDATE LOGIC';
PROC PRINT DATA=data_an.flag_transfers (OBS=200);
RUN;
TITLE;

/*DROP ADMIT, PROVID AND TXFLAG AFTER VALIDATING LOGIC*/
DATA data_an.flag_transfers;
	SET data_an.flag_transfers(DROP=admit provid txflag);
RUN;


**********************************************************************
* CREATE CMS_PROC VERSION FOR H/K AND CB
**********************************************************************;


/*DELETE WORK AND INITIAL DATASET TO CLEAN UP THE EG PROCESS FLOW*/
PROC DELETE DATA= flag_transfers;
RUN;
TITLE;

%macro nobs(data,name); /*Testing the observation counting macro. BR 5-10-17*/
%global &name.; 
	data _null_;
		if 0 then set &data. nobs=count; 
		call symput ("&name.",left(put(count,9.))); 
		stop; 
	run; 

	%put Summary Report - Number of obs in &data.: &obs.;
%mend nobs;
%nobs(data=data_sty.&stay_dataset.,name=obs); 
%nobs(data=data_an.flag_transfers,name=obs);


DATA _NULL_;
EndDate = "%SYSFUNC(DATE(),WORDDATE.)";
EndTime = "%SYSFUNC(TIME(),TIME.)";
PUT "===================================================================================";
PUT "  ENDING JOB INFORMATION:" /;
PUT "  Job Name: 0A_create_flag_transfers_dataset" /;
PUT "  End Date: " EndDate ;
PUT "  End Time: " EndTime ;
PUT "===================================================================================";
RUN;

