/************************************************************************************************************
                               SAS Program Description

PROGRAM NAME: 5_Post_Index

FOR USE WITH: AMI, HF, PNEUMPONIA, STROKE AND COPD

PURPOSE:      Create post index file 

OVERVIEW FROM SPECS: Select stays that are up to 365 days after each index stay. create dataset
					 for creating POST_FLAG and POST_INDEX_STAY flags in the index file.  
					 Remove same day and next day transfers and format data for post index file.

INPUT DATA: 
	data_sty.&stay_dataset.  *PRE-SORTED BY HICNO ADMIT DISCH TXFLAG PROVID
	dx_data.indexMatch       *PRE-SORTED BY HICNO

OUTPUT FILES: 
	dx_data.pstindex -for creating POST_FLAG and POST_INDEX_STAY flags in the index file.
	An_files.postindex_&DX._&YY.&YYE.

************************************************************************************************************/

OPTIONS COMPRESS=YES;

DATA _NULL_;
StartDate = "%SYSFUNC(DATE(),WORDDATE.)";
StartTime = "%SYSFUNC(TIME(),TIME.)";
PUT "===================================================================================";
PUT "  STARTING JOB INFORMATION:" /;
PUT "  Job Name: 5_Post_Index" /;
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
%remove(ds=An_files.postindex_&DX._&YY.&YYE.);
/**********************************************************************************************
**********************************************************************************************/

**********************************************************************
* CHECK MACRO VARIABLES
**********************************************************************;
%PUT DX (MEASURE VARIABLE)= &DX.;

%PUT DXN (MAX NUMBER OF DIAGNOSIS)= &DXN.;
%PUT PRN (MAX NUMBER OF PROCEDURE)= &PRN.;

%PUT YY (FIRST YEAR OF SPLIT YEAR PERIOD)= &YY.;
%PUT YYE (LAST YEAR OF SPLIT YEAR PERIOD)= &YYE.;
RUN;
**********************************************************************;

**********************************************************************
* DETERMINE THE MAXIMUM NUMBER OF CASES IN BUNDLED CLAIMS.
**********************************************************************;
OPTIONS OBS=1;
DATA _NULL_;
	SET dx_data.indexMatch;
	CALL SYMPUT("MC", LEFT(PUT(index_max_case, 3.)));
RUN;
OPTIONS OBS=MAX;
%PUT MAX CASE FOR INDEX: &MC.;
**********************************************************************;



**********************************************************************
* SELECT POST INDEX CLAIMS
**********************************************************************;
DATA pstindex (KEEP=admdiff admit admsour case ccuind ddest diag1-diag&DXN disch pstate
					disst drgcd dvrsnd01-dvrsnd25 edgscd01-edgscd12 evrscd01-evrscd12 hicno
					icuind los mdcl_rec poaend01-poaend12 poancd1-poancd25 proc1-proc&PRN
					procdt1-procdt&PRN provid pvsncd01-pvsncd25 typeadm txflag year same_provid factype sslssnf pmt_amt tot_chg);
	MERGE data_sty.stay_dataset_all (IN=INA WHERE = (admit > INTNX('MONTH',&BEGINDT,-6) /*and case_type='CMS'*/)
								  KEEP=admit admsour case_type ccuind ddest diag1-diag&DXN 
										disch disst drgcd dvrsnd01-dvrsnd25 edgscd01-edgscd12 
										evrscd01-evrscd12 hicno icuind los mdcl_rec 
										poaend01-poaend12 poancd1-poancd25 proc1-proc&PRN 
										procdt1-procdt&PRN provid pvsncd01-pvsncd25 typeadm 
										txflag factype sslssnf pmt_amt tot_chg pstate) 
		  dx_data.indexMatch(IN=INX KEEP=hicno disdt1-disdt&MC provid1-provid&MC); 
	BY hicno;
	IF (INX AND INA);

	LENGTH case 3 year 4;

	ARRAY DIS(&MC) disdt1-disdt&MC;
	ARRAY PVD(&MC) $provid1-provid&MC;

	DO I=1 TO &MC;

		/*DAYS BETWEEN INDEX DISCHARGE AND CLAIM ADMISSION*/
		admdiff=admit-DIS(I);


		/*CHECK FOR POST ADMISSION WITHIN 365 DAYS*/
		/*SAME OR NEXT DAY TRANSFERS ARE REMOVED IN A LATER STEP SO THIS FILE*/
		/*CAN BE USED TO SET THE POST_FLAG AND POST_INDEX_STAY IN THE INDEX*/
		IF DIS(I)>0 AND (0<=admdiff<=/*365*/180) THEN DO;

			case = I;
			IF provid = PVD(I) THEN same_provid=1;
			ELSE same_provid=0;

			/*CREATE YEAR VARIABLE - missing for prior to period*/
			year=year(DIS(I)); /*Changed year to match calendar year instead of medicare
					   enrollment year. BR 7/23/15*/
			/*
			IF "1jul20&yy"d <= DIS(I) <= "30jun%eval(20&yy+1)"d               
			  THEN year = "&yy"||SUBSTR("%EVAL(20&yy+1)",3,2);             
			ELSE IF "1jul%eval(20&yy+1)"d <= DIS(I) <= "30jun%eval(20&yy+2)"d 
			  THEN year = SUBSTR("%EVAL(20&yy+1)",3,2)||SUBSTR("%EVAL(20&yy+2)",3,2);                     
			ELSE IF "1jul%eval(20&yy+2)"d <= DIS(I) <= "30jun%eval(20&yy+3)"d 
			  THEN year = SUBSTR("%EVAL(20&yy+2)",3,2)||SUBSTR("%EVAL(20&yy+3)",3,2);                     
			*/

			OUTPUT pstindex;
		END;
	END;  /*END CASE NUMBER LOOP*/
RUN;


/*PERFORM DATA CHECKS*/
TITLE 'INITIAL POST INDEX CHECKS';
PROC PRINT DATA=pstindex(OBS=50);
	FORMAT admit disch MMDDYY8.; 
RUN;

TITLE 'FREQ OF ADMDIFF -1, 0';
PROC FREQ DATA=pstindex;
	TABLES admdiff/MISSING;
	WHERE admdiff IN (-1,0);
RUN;

TITLE 'CHECK OF ADMDIFF=0';
PROC PRINT DATA=pstindex(OBS=50);
	WHERE admdiff=0;
	FORMAT admit disch MMDDYY8.;
RUN;

TITLE 'CHECK OF ADMDIFF=-1';
PROC PRINT DATA=pstindex(OBS=50);
	WHERE admdiff=-1;
	FORMAT admit disch MMDDYY8.;
RUN;

TITLE 'FREQ OF POST INDEX ADMISSION DATE';
PROC FREQ DATA=pstindex;
	TABLES admit/MISSING;
	FORMAT admit MONYY.;
RUN;
**********************************************************************;



**********************************************************************
* CREATE DATASET FOR CREATING POST_FLAG AND POST_INDEX_STAY FLAGS IN THE INDEX FILE
* ONLY NEED STAYS WITH ADMIT IN INDEX PERIOD OR DAY AFTER AND DISCH AFTER INDEX PERIOD
* ONLY KEEP NEEDED FIELDS AND RECORDS
**********************************************************************;
 /*SORT STAYS BY HICNO, ADMIT, DISCH, TXFLAG AND PROVID*/
PROC SORT DATA=pstindex;
	BY hicno case admit disch txflag provid;
RUN;

/*FLAG INDEX STAYS*/
DATA pstindexIS(DROP=diag1);
	SET pstindex(KEEP=hicno admit disch txflag provid diag1 diag2 drgcd factype);
	WHERE admit <= &CLOSEDT <= disch & factype="STA"; /*Added in the STA stipulation for a transfer so we don't accidentally flag PAC as a transfer. BR 5-8-17*/

	/*FLAG STAYS THAT MEET DIAGNOSIS*/
	IF &&DIAGSELECT_&DX
	THEN index_stay=1;
RUN;

/*ONLY NEED FIRST CASE PER HICNO*/
PROC SORT 
	DATA=pstindexIS(KEEP=hicno admit index_stay provid
					RENAME=(admit=postadmit provid=postprovid))
	OUT=dx_data.pstindex
	NODUPKEY;
	BY hicno;
RUN;
**********************************************************************;



**********************************************************************
* FLAG TRANSFERS WITHIN POST INDEX FILE AND FORMAT FIELDS
**********************************************************************;
 /*ASSIGN AN OBSERVATION NUMBER TO STAYS AND THEN SORT BY THE OBS NUMBER*/
DATA pstindex_nobs
	 deleteIndexTransfer
	 deleteDuplicates;
	SET pstindex;

	/*CREATE CLAIM OBSERVATION NUMBER TO USE BELOW TO FLAG
	 CLAIMS IN A TRANSFER CHAIN*/
	nobs=_N_;

	/*REMOVE TRANSFERS TO POST INDEX FROM READMISSION INDEX*/
	IF factype ~in("SNF","IRF","LTC","HHA","OUT","MDS") AND same_provid = 0 AND admdiff <= 1 THEN OUTPUT deleteIndexTransfer;

	/*REMOVE DUPLICATES OF ONE DAY STAYS IN THE INDEX*/
	ELSE IF factype ~in("SNF","IRF","LTC","HHA","OUT","MDS") AND same_provid = 1 AND admit = disch AND admdiff = 0 
	THEN OUTPUT deleteDuplicates;

	ELSE OUTPUT pstindex_nobs;
RUN;

 /*FLAG CLAIMS THAT MAKE UP A TRANSFER CHAIN*/
DATA flag_transfers
	(KEEP=newobs RENAME=(newobs=nobs));

   /*CREATE LAGGED VARIABLES*/
   LENGTH mark $ 1 hicno2 $ 15 prvid2 $ 6;
   RETAIN mark '';

   prvid2 = provid;
   hicno2 = hicno;
   case2 = case;
   disdate2 = disch;
   nobs2 = nobs;
   txflag2 = txflag;

   SET pstindex_nobs;

   /*SELECT TRANSFER CLAIMS*/
	/*DIFFERENT PROVIDER, SAME OR NEXT DAY ADMIT AFTER DISCHARGE, NOT TO PAC, AND TXFLAG OF 0 OR -1*/
	IF hicno2 = hicno AND case2 = case AND prvid2 NE provid AND (disdate2 <= admit <= disdate2+1) AND 
	   factype ~in("SNF","IRF","LTC","HHA","OUT","MDS") AND txflag2<1 THEN DO;
		IF mark = '' THEN DO;  /*IF FIRST IDENTIFIED THEN OUTPUT CURRENT AND PREVIOUS CASES*/
			mark = '1';
			newobs = nobs2;
			OUTPUT;
			newobs = nobs;
			OUTPUT;
		END;
		ELSE IF mark = '1' THEN DO;  /*IF PREVIOUS CASE WAS OUTPUT THEN ONLY OUTPUT CURRENT CASE*/
			newobs=nobs;
			OUTPUT;
		END;
	END;
	ELSE DO;  /*RESET MARK IF NOT A TRANSFER CASE*/
		mark='';
	END;
RUN;

/*FLAG ALL CLAIMS IN A TRANSFER CHAIN*/
DATA An_files.postindex_&DX._&YY.&YYE.
		(KEEP = admdiff admit admsour case ccuind ddest diag1-diag&DXN. disch disst pstate
				drgcd dvrsnd01-dvrsnd25 edgscd01-edgscd12 evrscd01-evrscd12 hicno icuind los 
				mdcl_rec poaend01-poaend12 poancd1-poancd25 proc1-proc&PRN. procdt1-procdt&PRN.
				provid pvsncd01-pvsncd25 trans typeadm year factype sslssnf pmt_amt tot_chg);
	MERGE pstindex_nobs 
	      flag_transfers (IN=t KEEP=nobs);
	BY nobs;

	/*FLAG TRANS*/
	IF t THEN trans=1;
	ELSE trans=0;

	/*REFORMAT VARS*/
	LENGTH	ccuind2 icuind2 3 ddest2 drgcd2 8 provid2 $6. nprocdt1-nprocdt&PRN. $8.;

	ddest2 = input(ddest, 8.);
	ccuind2 = ccuind;
	drgcd2 = input(drgcd, 8.);
	icuind2 = icuind;
	provid2 = substr(left(provid), 1, 6);

	ARRAY PROC procdt1-procdt&PRN.;
	ARRAY NPRC nprocdt1-nprocdt&PRN.;

	DO I = 1 TO &PRN.; 
		IF PROC(I)=. THEN NPRC(I)='';
		ELSE NPRC(I)=put(PROC(I),YYMMDDN8.);
	END;

	DROP ddest ccuind drgcd icuind provid procdt1-procdt&PRN.;

	RENAME	ddest2		= ddest
			ccuind2		= ccuind
			drgcd2		= drgcd 
			icuind2		= icuind 
			provid2		= provid
			nprocdt1-nprocdt&PRN. = procdt1-procdt&PRN.;
 
	LABEL
		admdiff     = 'Days between index discharge and claim admission'
		admit       = 'Admission date'
		admsour     = 'Source of admission'
		case        = 'Case number from Index Event Record'
		ccuind2     = 'Coronary Care Unit indicator'
		ddest2      = 'Discharge destination code'
		disch       = 'Discharge date'
		drgcd2      = 'Diagnosis Related Group'
		disst       = 'Medicare discharge status code'
		icuind2     = 'Intensive Care Unit indicator'
		mdcl_rec    = 'Medical record number'
		typeadm     = 'Type of admission'
		provid2     = 'Medicare provider number'
		trans       = '1=stay is part of a transfer chain'
		hicno       = 'CLEAN XREFD ID'
		los         = 'Length of stay'
	%MACRO labelcounts();
		%DO I=1 %TO &DXN;
			diag&I   = "Diagnosis code #&I"
		%END;

		%DO J=1 %TO &PRN;
			proc&J   = "Procedure code #&J"
			nprocdt&J = "Procedure date #&J"
		%END;

		%DO I=1 %TO 25;
			%LET N=%SYSFUNC(PUTN(&I,Z2.));
			poancd&I = "Diagnosis code POA indicator #&I"
			dvrsnd&N = "Diagnosis code version indicator #&I"
			pvsncd&N = "Procedure code version indicator #&I"
		%END;

		%DO M=1 %TO 12;
			%LET N=%SYSFUNC(PUTN(&M,Z2.));
			edgscd&N = "External cause of injury diagnosis code #&N"
			evrscd&N = "Diagnosis code version indicator  #&N"
			poaend&N = "External cause of injury POA indicator #&N"
		%END;
	%MEND labelcounts;
	%labelcounts;;


	/*ADD case_type FOR AMI, HF AND PN*/
	/*case_type='CMS';*/
RUN;


/*SORT POST INDEX*/
PROC SORT DATA= An_files.postindex_&DX._&YY.&YYE.;
	BY hicno case;
RUN;

**********************************************************************
* PERFORM DATA CHECKS
**********************************************************************;

TITLE "postindex_&DX._&YY.&YYE.";
PROC CONTENTS DATA=An_files.postindex_&DX._&YY.&YYE.;
RUN;

PROC FREQ DATA=An_files.postindex_&DX._&YY.&YYE.;
	TABLES admit*disch
		   trans*ddest 
	/LIST MISSING;
	FORMAT admit disch year4.;
RUN;

PROC FREQ DATA=An_files.postindex_&DX._&YY.&YYE.;
	TABLES case admsour icuind ccuind typeadm ddest
	/LIST MISSING;
RUN;

PROC MEANS DATA=An_files.postindex_&DX._&YY.&YYE.;
	VAR case los admdiff;
RUN;

PROC PRINT DATA=An_files.postindex_&DX._&YY.&YYE. (OBS=20);
	FORMAT admit disch mmddyy8.;
RUN;

TITLE "POST INDEX QA OF TRANS=1";
PROC PRINT DATA=An_files.postindex_&DX._&YY.&YYE. (OBS=20);
	VAR hicno admit disch provid trans ddest;
	WHERE trans=1;
	FORMAT admit disch mmddyy10.;
RUN;

TITLE "CHECK OF DELETED TRANSFER CASES";
PROC FREQ DATA=deleteIndexTransfer;
	TABLES same_provid*admdiff/ LIST MISSING;
RUN;

TITLE "CHECK OF DELETED ONE DAY STAY DUPLICATES OF INDEX CASES CASES";
PROC FREQ DATA=deleteDuplicates;
	TABLES same_provid*admdiff*disch/ LIST MISSING;
	FORMAT disch MONYY.;
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
%nobs(data=data_sty.stay_dataset_all,name=obs); 
%nobs(data=dx_data.indexMatch,name=obs); 
%nobs(data=An_files.postindex_&DX._&YY.&YYE.,name=obs); 

/*DELETE WORK AND INITIAL DATASET TO CLEAN UP THE EG PROCESS FLOW*/
PROC DELETE DATA=pstindex pstindexIS pstindex_nobs flag_transfers deleteIndexTransfer deleteDuplicates; 
RUN;
TITLE;

DATA _NULL_;
EndDate = "%SYSFUNC(DATE(),WORDDATE.)";
EndTime = "%SYSFUNC(TIME(),TIME.)";
PUT "===================================================================================";
PUT "  ENDING JOB INFORMATION:" /;
PUT "  Job Name: 5_Post_Index" /;
PUT "  End Date: " EndDate ;
PUT "  End Time: " EndTime ;
PUT "===================================================================================";
RUN;
